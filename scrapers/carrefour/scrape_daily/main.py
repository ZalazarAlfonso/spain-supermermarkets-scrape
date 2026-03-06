#!/usr/bin/env python3
"""
Carrefour Spain (carrefour.es) supermarket product scraper.

- Focuses on Frescos, La Despensa, and Bebidas groups
- Discovers category pages per group, then subcategories within each category
- Crawls category pages + pagination
- Parses product cards directly (no product page fetch needed)
- Writes a daily CSV with columns:
  date, product, brand, price, price_per_unit, offer, category, subcategory, product_url

Notes:
- This script is intentionally conservative about rate-limiting.
- It relies on HTML scraping (no public JSON endpoints found in page source).
- Override group URLs with CARREFOUR_GROUP_URLS (comma-separated).
- If you get 403s or missing content, the scraper falls back to Playwright rendering (install: pip install playwright && playwright install).
"""

import argparse
import csv
import datetime as dt
import os
import json
from jsonschema import validate
import sys
import time
from typing import Dict, List, Optional, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from scrapers.carrefour.common import config as cfg
from scrapers.carrefour.common import gcs, parsing, http
from scrapers.carrefour.common.models import ProductRow
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def load_targets(args) -> dict:
    """
    Load targets from local or gcs.

    Args:
        args: Depending on the location of it it will take it form gcs or form local.
    
    Return:
        json with the targets.
    """
    schema = {
        "type" : "object",
        "properties" : {
            "generated_at_utc" : {"type" : "string"},
            "source_group_urls" : {
                "type": "array",
                "items": {"type": "string"},
            },
            "count" : {"type" : "number"},
            "targets": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "group": {"type": "string"},
                        "category": {"type": "string"},
                        "subcategory": {"type": "string"},
                        "url": {"type": "string"},
                    },
                    "required": ["group", "category", "subcategory", "url"],
                },
                "minItems": 1,
            },
        },
        "required":["targets"]
    }
    if args.targets_source == "local":
        with open(args.targets_local_path, "r", encoding="utf-8") as f:
            targets = json.load(f)
            validate(instance=targets,schema=schema,)
            return targets

    # gcs
    if not args.targets_gcs_bucket or not args.targets_gcs_object_name:
        raise ValueError("both --targets-gcs-bucket and --targets-gcs-object-name are required when --targets-source=gcs")
    
    text = gcs.read_file_text(args.targets_gcs_bucket,args.targets_gcs_object_name)

    targets = json.loads(text)

    validate(instance=targets,schema=schema,)

    return targets

def scrape_category(
    session: requests.Session,
    category_label: str,
    subcategory_label: str,
    category_url: str,
    sleep_s: float,
    max_pages: Optional[int],
) -> List[ProductRow]:
    rows: List[Dict[str, str]] = []

    seen_pages: Set[str] = set()
    page_queue: List[str] = [category_url]

    while page_queue:
        if max_pages is not None and len(seen_pages) >= max_pages:
            break
        page_url = page_queue.pop(0)
        if page_url in seen_pages:
            continue
        seen_pages.add(page_url)

        try:
            page_html = http.fetch(session, page_url)
            time.sleep(max(0.4, sleep_s * 0.6) + random.random() * 0.4)
        except Exception as exc:
            print(
                f"[ERROR] page fetch failed category={category_label} "
                f"subcategory={subcategory_label or '-'} url={page_url}: {exc}"
            )
            continue
        page_soup = parsing.soup_from_html(page_html)

        cards = page_soup.select("div.product-card")
        print(
            f"[PAGE] {category_label} | {subcategory_label or '-'} -> {page_url} "
            f"cards={len(cards)}"
        )
        if not cards:
            product_urls = parsing.extract_product_links(page_soup)
            if not product_urls:
                product_urls = parsing.extract_product_links_from_html(page_html)
            for p_url in product_urls:
                rows.append(
                    {
                        "product": "",
                        "brand": "",
                        "price": "",
                        "price_per_unit": "",
                        "offer": "false",
                        "category": category_label,
                        "subcategory": subcategory_label,
                        "product_url": p_url,
                    }
                )
        else:
            for card in cards:
                name, brand, price, price_per_unit, offer = parsing.parse_product_card(card)
                link = ""
                link_el = card.select_one("a.product-card__title-link") or card.select_one("a.product-card__media-link")
                if link_el:
                    link = parsing.normalize_url(link_el.get("href", "")) or ""

                rows.append(
                    {
                        "product": name,
                        "brand": brand,
                        "price": price,
                        "price_per_unit": price_per_unit,
                        "offer": "true" if offer else "false",
                        "category": category_label,
                        "subcategory": subcategory_label,
                        "product_url": link,
                    }
                )

        for next_url in parsing.extract_pagination_links(page_soup, category_url):
            if next_url not in seen_pages and next_url not in page_queue:
                page_queue.append(next_url)
        print(f"[PAGINATION] {category_label} -> queued={len(page_queue)} seen={len(seen_pages)}")

        time.sleep(sleep_s)

    return rows

def main() -> int:
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="/tmp/carrefour", help="Output directory")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests (seconds)")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--max-categories", type=int, default=None)
    parser.add_argument("--allow-duplicates", action="store_true", help="Allow duplicates across categories")
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    parser.add_argument(
        "--targets-source",
        choices=["local", "gcs"],
        default=os.getenv("TARGETS_SOURCE", "gcs"),
        help="Where to read category targets from",
    )
    parser.add_argument(
        "--targets-local-path",
        default="scrapers/carrefour/scrape_daily/files/carrefour_categories.json",
    )
    parser.add_argument(
        "--targets-gcs-bucket",
        default=os.getenv("TARGETS_GCS_BUCKET", cfg.GCS_BUCKET),
        help="Bucket where the categories lives.",
    )
    parser.add_argument(
        "--targets-gcs-object-name",
        default=os.getenv("TARGETS_GCS_OBJECT_NAME", "carrefour/carrefour_categories.json"),
        help="Object Name.",
    )

    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = dt.date.today().isoformat()
    csv_filename = f"carrefour_supermercado_{today}.csv"
    parquet_filename = f"carrefour_supermercado_{today}.parquet"
    out_path = os.path.join(out_dir, csv_filename)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": cfg.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
    )
    
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={'GET'},
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    rows: List[Dict[str, str]] = []
    seen_products: Set[str] = set()
    
    # Error variables initialization.
    error_count = 0
    error_samples = []

    # Target extraction.
    data = load_targets(args)

    targets = data.get("targets", [])

    if args.max_categories is not None:
        allowed_categories = []
        seen_categories = set()

        for t in targets:
            cat = (t.get("category") or "").strip()
            if not cat or cat in seen_categories:
                continue
            seen_categories.add(cat)
            allowed_categories.append(cat)
            if len(allowed_categories) >= args.max_categories:
                break

        allowed_set = set(allowed_categories)
        targets = [t for t in targets if (t.get("category") or "").strip() in allowed_set]


    # Targets Scraping.
    for target in targets:
        if args.max_products is not None and len(rows) >= args.max_products:
            break
        try:
            print(
                f"[SCRAPE] category='{target['category']}' subcategory='{target['subcategory'] or '-'}' "
                f"url={target['url']}"
            )
            cat_rows = scrape_category(
                session, target['category'], target['subcategory'], target['url'], args.sleep, args.max_pages
            )
        except Exception as e:
            error_count += 1
            print(f"[ERROR] scrape_category failed url={target['url']} category={target['category']}: {e}")
            continue

        print(f"Scraping {target['category']} with {len(cat_rows)} products")
        for row in cat_rows:
            if args.max_products is not None and len(rows) >= args.max_products:
                break
            p_url = row.get("product_url", "")
            if not args.allow_duplicates and p_url and p_url in seen_products:
                continue
            if not args.allow_duplicates and p_url:
                seen_products.add(p_url)
            row["date"] = dt.date.today()
            rows.append(row)

    # Write CSV
    fieldnames = [
        "date",
        "product",
        "brand",
        "price",
        "price_per_unit",
        "offer",
        "category",
        "subcategory",
        "product_url",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Loading into pandas dataframe
    df = pd.read_csv(out_path)
    
    # Date formating.
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # converting to parquet
    table = pa.Table.from_pandas(df)

    # saving to parquet
    parquet_file_path = os.path.join(out_dir,parquet_filename)
    pq.write_table(table,parquet_file_path)
    object_name = f"carrefour/{today}/{parquet_filename}"

    # Upload to GCS.
    if args.upload_to_gcs:
        gcs.validate_gcs_upload_config(parquet_file_path, cfg.GCS_BUCKET, object_name)
        try:
            uri = gcs.upload_file(
                local_path=parquet_file_path,
                bucket_name=cfg.GCS_BUCKET,
                object_name=object_name,
                object_type="application/octet-stream"
            )
            print(f"[UPLOAD] OK -> {uri}")
        except Exception as exc:
            raise RuntimeError(
                f"GCS upload failed (bucket={cfg.GCS_BUCKET}, object={object_name}, file={parquet_file_path})"
            ) from exc

    # Clean up of the files
    if args.upload_to_gcs and cfg.KEEP_LOCAL_FILES != "true":
        os.remove(out_path)
        os.remove(parquet_file_path)
        print(f"[CLEANUP] deleted {out_path} and {parquet_file_path}")
    else:
        print('[CLEANUP] KEEP_LOCAL_FILES=true, skipping delete')

    # Final report metrics.
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"[SUMMARY] error_count={error_count}")
    print(f"Wrote {len(rows)} rows to {out_path}. Time : {(elapsed_time)/60} minutes")
    return 0

if __name__ == "__main__":
    sys.exit(main())

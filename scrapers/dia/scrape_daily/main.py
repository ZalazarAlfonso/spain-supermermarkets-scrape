#!/usr/bin/env python3
"""
Dia (dia.es) supermarket product scraper.

- Reads category targets from a JSON file (produced by targets_weekly)
- For each category URL, uses Playwright scrolling to capture products
- Parses product cards and falls back to JSON-LD product payloads
- Writes a daily CSV with columns:
  date, product, brand, price, price_per_unit, offer, category, subcategory, product_url
"""

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
from typing import Dict, List, Optional, Set

from jsonschema import validate

from scrapers.dia.common import config as cfg
from scrapers.dia.common import gcs, http, parsing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def load_targets(args) -> dict:
    """Load targets from local file or GCS."""
    schema = {
        "type": "object",
        "properties": {
            "generated_at_utc": {"type": "string"},
            "source_group_urls": {
                "type": "array",
                "items": {"type": "string"},
            },
            "count": {"type": "number"},
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
        "required": ["targets"],
    }

    if args.targets_source == "local":
        if not os.path.isfile(args.targets_local_path):
            raise FileNotFoundError(
                f"Targets file not found at '{args.targets_local_path}'. "
                "Run targets_weekly first, or use --targets-source gcs."
            )
        with open(args.targets_local_path, "r", encoding="utf-8") as f:
            targets = json.load(f)
            validate(instance=targets, schema=schema)
            return targets

    if not args.targets_gcs_bucket or not args.targets_gcs_object_name:
        raise ValueError(
            "both --targets-gcs-bucket and --targets-gcs-object-name are required "
            "when --targets-source=gcs"
        )

    text = gcs.read_file_text(args.targets_gcs_bucket, args.targets_gcs_object_name)
    targets = json.loads(text)
    validate(instance=targets, schema=schema)
    return targets


def _merge_product_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    by_url: Dict[str, Dict[str, str]] = {}
    by_name: Dict[str, Dict[str, str]] = {}

    for row in rows:
        url = (row.get("product_url") or "").strip()
        name = (row.get("product") or "").strip().lower()
        if url:
            by_url[url] = row
        elif name:
            by_name[name] = row

    used_names = {(r.get("product") or "").strip().lower() for r in by_url.values()}
    merged = list(by_url.values())
    for name, row in by_name.items():
        if name not in used_names:
            merged.append(row)

    return merged


def scrape_category(
    category_label: str,
    subcategory_label: str,
    category_url: str,
    sleep_s: float,
    max_products: Optional[int],
) -> List[Dict[str, str]]:
    """Scrape all products from a single Dia category page."""
    rows: List[Dict[str, str]] = []

    try:
        products = http.fetch_scrolled_products(category_url)
    except Exception as exc:
        print(
            f"[ERROR] fetch_scrolled_products failed category={category_label} "
            f"subcategory={subcategory_label or '-'} url={category_url}: {exc}"
        )
        products = []

    if products:
        print(
            f"[PAGE] {category_label} | {subcategory_label or '-'} -> {category_url} "
            f"products={len(products)}"
        )
        for product in products:
            if max_products is not None and len(rows) >= max_products:
                break
            rows.append(
                {
                    "product": product.get("product", ""),
                    "brand": product.get("brand", ""),
                    "price": product.get("price", ""),
                    "price_per_unit": product.get("price_per_unit", ""),
                    "offer": product.get("offer", "false"),
                    "category": category_label,
                    "subcategory": subcategory_label,
                    "product_url": product.get("product_url", ""),
                }
            )

        time.sleep(sleep_s)
        return _merge_product_rows(rows)

    # Fallback path: parse final HTML snapshot for cards + json-ld.
    try:
        page_html = http.fetch_scrolled(category_url)
        page_soup = parsing.soup_from_html(page_html)
        cards = parsing.extract_product_cards(page_soup)
        json_ld_products = parsing.extract_products_from_json_ld(page_soup)

        print(
            f"[PAGE] {category_label} | {subcategory_label or '-'} -> {category_url} "
            f"cards={len(cards)} jsonld={len(json_ld_products)} (fallback-html)"
        )

        if not cards and not json_ld_products:
            markers = parsing.detect_page_markers(page_soup)
            print(
                f"[WARN] no product cards detected "
                f"location_gate={markers['location_gate']} "
                f"empty_results={markers['empty_results']} "
                f"blocked={markers['blocked']} "
                f"url={category_url}"
            )

        for card in cards:
            if max_products is not None and len(rows) >= max_products:
                break
            name, brand, price, price_per_unit, offer = parsing.parse_product_card(card)
            product_url = parsing.extract_product_url(card)
            if not name and not product_url:
                continue
            rows.append(
                {
                    "product": name,
                    "brand": brand,
                    "price": price,
                    "price_per_unit": price_per_unit,
                    "offer": "true" if offer else "false",
                    "category": category_label,
                    "subcategory": subcategory_label,
                    "product_url": product_url,
                }
            )

        for product in json_ld_products:
            if max_products is not None and len(rows) >= max_products:
                break
            rows.append(
                {
                    "product": product.get("product", ""),
                    "brand": product.get("brand", ""),
                    "price": product.get("price", ""),
                    "price_per_unit": product.get("price_per_unit", ""),
                    "offer": product.get("offer", "false"),
                    "category": category_label,
                    "subcategory": subcategory_label,
                    "product_url": product.get("product_url", ""),
                }
            )

    except Exception as exc:
        print(
            f"[ERROR] fetch_scrolled fallback failed category={category_label} "
            f"subcategory={subcategory_label or '-'} url={category_url}: {exc}"
        )
        return []

    time.sleep(sleep_s)
    return _merge_product_rows(rows)


def main() -> int:
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="/tmp/dia", help="Output directory")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between category requests (seconds)")
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
    parser.add_argument("--targets-local-path", default="scrapers/dia/targets_weekly/files/dia_categories.json")
    parser.add_argument(
        "--targets-gcs-bucket",
        default=os.getenv("TARGETS_GCS_BUCKET", cfg.GCS_BUCKET),
        help="Bucket where the categories JSON lives.",
    )
    parser.add_argument(
        "--targets-gcs-object-name",
        default=os.getenv("TARGETS_GCS_OBJECT_NAME", "dia/dia_categories.json"),
        help="GCS object name for the categories JSON.",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = dt.date.today().isoformat()
    csv_filename = f"dia_supermercado_{today}.csv"
    parquet_filename = f"dia_supermercado_{today}.parquet"
    out_path = os.path.join(out_dir, csv_filename)

    rows: List[Dict[str, str]] = []
    seen_products: Set[str] = set()
    error_count = 0

    data = load_targets(args)
    targets = data.get("targets", [])

    if args.max_categories is not None:
        allowed_categories = []
        seen_categories: Set[str] = set()

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

    for target in targets:
        if args.max_products is not None and len(rows) >= args.max_products:
            break
        try:
            print(
                f"[SCRAPE] category='{target['category']}' subcategory='{target['subcategory'] or '-'}' "
                f"url={target['url']}"
            )
            cat_rows = scrape_category(
                category_label=target["category"],
                subcategory_label=target["subcategory"],
                category_url=target["url"],
                sleep_s=args.sleep,
                max_products=args.max_products,
            )
        except Exception as exc:
            error_count += 1
            print(f"[ERROR] scrape_category failed url={target['url']} category={target['category']}: {exc}")
            continue

        print(f"Scraped {target['category']} with {len(cat_rows)} products")
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

    object_name = f"dia/{today}/{parquet_filename}"
    upload_env = os.getenv("UPLOAD_TO_GCS", "true").strip().lower() == "true"
    if args.upload_to_gcs or upload_env:
        gcs.validate_gcs_upload_config(parquet_file_path, cfg.GCS_BUCKET, object_name)
        try:
            uri = gcs.upload_file(
                local_path=parquet_file_path,
                bucket_name=cfg.GCS_BUCKET,
                object_name=object_name,
                object_type="application/octet-stream",
            )
            print(f"[UPLOAD] OK -> {uri}")
        except Exception as exc:
            raise RuntimeError(
                f"GCS upload failed (bucket={cfg.GCS_BUCKET}, object={object_name}, file={parquet_file_path})"
            ) from exc
    did_upload = args.upload_to_gcs or upload_env
    if did_upload and cfg.KEEP_LOCAL_FILES != "true":
        os.remove(out_path)
        os.remove(parquet_file_path)
        print(f"[CLEANUP] deleted {out_path} and {parquet_file_path}")
    else:
        print("[CLEANUP] KEEP_LOCAL_FILES=true, skipping delete")

    elapsed = time.perf_counter() - start_time
    print(f"[SUMMARY] error_count={error_count}")
    print(f"Wrote {len(rows)} rows to {out_path}. Time: {elapsed / 60:.1f} minutes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

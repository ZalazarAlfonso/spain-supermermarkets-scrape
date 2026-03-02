#!/usr/bin/env python3
"""
Mercadona (tienda.mercadona.es) supermarket product scraper.

- Reads category targets from a JSON file (produced by targets_weekly)
- Fetches category JSON from Mercadona public API
- Flattens product records into CSV with columns:
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

from jobs.mercadona.common import config as cfg
from jobs.mercadona.common import gcs, http, parsing

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


def fetch_product_brand(
    session,
    product_id: str,
    cache: Dict[str, str],
) -> str:
    """Fetch product details and return explicit brand, with cache."""
    if product_id in cache:
        return cache[product_id]

    url = cfg.PRODUCT_API_URL_TEMPLATE.format(product_id=product_id)
    try:
        data = http.fetch_json(session, url, timeout=cfg.REQUEST_TIMEOUT_S)
    except Exception:
        cache[product_id] = ""
        return ""

    brand = str(data.get("brand") or "").strip()
    cache[product_id] = brand
    return brand


def scrape_category(
    session,
    category_label: str,
    subcategory_label: str,
    category_url: str,
    sleep_s: float,
    max_products: Optional[int],
    fetch_brand_details: bool,
    brand_cache: Dict[str, str],
) -> List[Dict[str, str]]:
    """Scrape all products from one Mercadona category endpoint."""
    rows: List[Dict[str, str]] = []

    try:
        payload = http.fetch_json(session, category_url, timeout=cfg.REQUEST_TIMEOUT_S)
    except Exception as exc:
        print(
            f"[ERROR] category fetch failed category={category_label} "
            f"subcategory={subcategory_label or '-'} url={category_url}: {exc}"
        )
        return rows

    leaf_buckets = payload.get("categories") or []
    if not isinstance(leaf_buckets, list):
        leaf_buckets = []

    total_products = 0
    for leaf in leaf_buckets:
        if max_products is not None and len(rows) >= max_products:
            break
        if not isinstance(leaf, dict):
            continue

        leaf_name = str(leaf.get("name") or "").strip()
        merged_subcategory = parsing.build_subcategory_label(subcategory_label, leaf_name)
        products = leaf.get("products") or []
        if not isinstance(products, list):
            products = []

        for p in products:
            if max_products is not None and len(rows) >= max_products:
                break
            if not isinstance(p, dict):
                continue
            name = str(p.get("display_name") or "").strip()
            brand = parsing.extract_brand(p)
            if fetch_brand_details and not brand:
                product_id = str(p.get("id") or "").strip()
                if product_id:
                    brand = fetch_product_brand(session, product_id, brand_cache)
            price, price_per_unit, offer = parsing.extract_price_fields(p)
            product_url = parsing.extract_product_url(p)
            rows.append(
                {
                    "product": name,
                    "brand": brand,
                    "price": price,
                    "price_per_unit": price_per_unit,
                    "offer": "true" if offer else "false",
                    "category": category_label,
                    "subcategory": merged_subcategory,
                    "product_url": product_url,
                }
            )
            total_products += 1

    print(
        f"[PAGE] {category_label} | {subcategory_label or '-'} -> {category_url} "
        f"products={total_products}"
    )
    if sleep_s > 0:
        time.sleep(sleep_s)
    return rows


def main() -> int:
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="/tmp/mercadona", help="Output directory")
    parser.add_argument("--sleep", type=float, default=cfg.REQUEST_SLEEP_S, help="Sleep between category requests")
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--max-categories", type=int, default=None)
    parser.add_argument("--allow-duplicates", action="store_true", help="Allow duplicates across categories")
    parser.add_argument(
        "--fetch-brand-details",
        action="store_true",
        help="Fetch /api/products/{id}/ when brand is missing in category payload.",
    )
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    parser.add_argument(
        "--targets-source",
        choices=["local", "gcs"],
        default=os.getenv("TARGETS_SOURCE", "gcs"),
        help="Where to read category targets from",
    )
    parser.add_argument(
        "--targets-local-path",
        default="jobs/mercadona/targets_weekly/files/mercadona_categories.json",
    )
    parser.add_argument(
        "--targets-gcs-bucket",
        default=os.getenv("TARGETS_GCS_BUCKET", cfg.GCS_BUCKET),
        help="Bucket where the categories JSON lives.",
    )
    parser.add_argument(
        "--targets-gcs-object-name",
        default=os.getenv("TARGETS_GCS_OBJECT_NAME", "mercadona/mercadona_categories.json"),
        help="GCS object name for the categories JSON.",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = dt.date.today().isoformat()
    csv_filename = f"mercadona_supermercado_{today}.csv"
    parquet_filename = f"mercadona_supermercado_{today}.parquet"
    out_path = os.path.join(out_dir, csv_filename)

    rows: List[Dict[str, str]] = []
    seen_products: Set[str] = set()
    error_count = 0
    brand_cache: Dict[str, str] = {}

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

    session = http.build_session()

    for target in targets:
        if args.max_products is not None and len(rows) >= args.max_products:
            break
        try:
            print(
                f"[SCRAPE] category='{target['category']}' subcategory='{target['subcategory'] or '-'}' "
                f"url={target['url']}"
            )
            cat_rows = scrape_category(
                session=session,
                category_label=target["category"],
                subcategory_label=target["subcategory"],
                category_url=target["url"],
                sleep_s=args.sleep,
                max_products=args.max_products,
                fetch_brand_details=args.fetch_brand_details,
                brand_cache=brand_cache,
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

    object_name = f"mercadona/{today}/{parquet_filename}"
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
    print(f"Wrote {len(rows)} rows to {parquet_file_path}. Time: {elapsed / 60:.1f} minutes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

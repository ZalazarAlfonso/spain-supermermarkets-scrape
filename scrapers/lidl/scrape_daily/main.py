#!/usr/bin/env python3
"""
Lidl Spain (lidl.es) structured product scraper.

- Reads targets from a JSON file produced by targets_weekly
- Scrapes structured online category API targets
- Skips weekly leaflet metadata targets (group="leaflet")
- Writes CSV and Parquet with standard supermarket columns
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

from scrapers.lidl.common import config as cfg
from scrapers.lidl.common import gcs, http, parsing

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


def scrape_category(
    session,
    category_label: str,
    subcategory_label: str,
    category_url: str,
    sleep_s: float,
    max_products: Optional[int],
) -> List[Dict[str, str]]:
    """Scrape all structured products from one Lidl online API category target."""
    rows: List[Dict[str, str]] = []
    offset = 0
    fetch_size = cfg.ONLINE_FETCH_SIZE
    seen_offsets: Set[int] = set()

    while True:
        if max_products is not None and len(rows) >= max_products:
            break
        if offset in seen_offsets:
            break
        seen_offsets.add(offset)

        page_url = parsing.paged_api_url(category_url, offset=offset, fetch_size=fetch_size)
        try:
            payload = http.fetch_json(session, page_url, timeout=cfg.REQUEST_TIMEOUT_S)
        except Exception as exc:
            print(
                f"[ERROR] category fetch failed category={category_label} "
                f"subcategory={subcategory_label or '-'} url={page_url}: {exc}"
            )
            break

        page_count = 0
        for item in parsing.iter_product_items(payload):
            if max_products is not None and len(rows) >= max_products:
                break
            row = parsing.product_row_from_item(item, category_label, subcategory_label)
            if not row:
                continue
            rows.append(row)
            page_count += 1

        num_found = int(payload.get("numFound") or 0)
        response_fetch_size = int(payload.get("fetchsize") or payload.get("fetchSize") or fetch_size)
        response_offset = int(payload.get("offset") or offset)

        print(
            f"[PAGE] {category_label} | {subcategory_label or '-'} -> {page_url} "
            f"products={page_count} offset={response_offset} num_found={num_found}"
        )

        if sleep_s > 0:
            time.sleep(sleep_s)

        if page_count == 0:
            break
        next_offset = response_offset + max(1, response_fetch_size)
        if num_found and next_offset >= num_found:
            break
        offset = next_offset

    return rows


def main() -> int:
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="scrapers/lidl/scrape_daily/files", help="Output directory")
    parser.add_argument("--sleep", type=float, default=cfg.REQUEST_SLEEP_S, help="Sleep between category requests")
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
        default="scrapers/lidl/targets_weekly/files/lidl_categories.json",
    )
    parser.add_argument(
        "--targets-gcs-bucket",
        default=os.getenv("TARGETS_GCS_BUCKET", cfg.GCS_BUCKET),
        help="Bucket where the categories JSON lives.",
    )
    parser.add_argument(
        "--targets-gcs-object-name",
        default=os.getenv("TARGETS_GCS_OBJECT_NAME", "lidl/lidl_categories.json"),
        help="GCS object name for the categories JSON.",
    )
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = dt.date.today().isoformat()
    csv_filename = f"lidl_supermercado_{today}.csv"
    parquet_filename = f"lidl_supermercado_{today}.parquet"
    out_path = os.path.join(out_dir, csv_filename)

    rows: List[Dict[str, str]] = []
    seen_products: Set[str] = set()
    error_count = 0

    data = load_targets(args)
    targets = [
        t
        for t in data.get("targets", [])
        if (t.get("group") or "").strip().lower() != "leaflet"
    ]
    skipped_leaflets = len(data.get("targets", [])) - len(targets)
    if skipped_leaflets:
        print(f"[INFO] skipped leaflet metadata targets={skipped_leaflets}")

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

    df = pd.read_csv(out_path)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    table = pa.Table.from_pandas(df)

    parquet_file_path = os.path.join(out_dir, parquet_filename)
    pq.write_table(table, parquet_file_path)

    object_name = f"lidl/{today}/{parquet_filename}"
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


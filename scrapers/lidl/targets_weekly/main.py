#!/usr/bin/env python3
"""Build a reusable Lidl Spain target file."""

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

from scrapers.lidl.common import config as cfg
from scrapers.lidl.common import gcs, http, parsing
from scrapers.lidl.common.models import CategoryTarget


def discover_online_targets(session, source_urls: List[str], sleep_s: float) -> List[CategoryTarget]:
    """Discover structured online category API targets."""
    targets: List[CategoryTarget] = []
    seen: Set[str] = set()

    for page_url in source_urls:
        print(f"[ONLINE] discover url={page_url}")
        try:
            html = http.fetch_text(session, page_url, timeout=cfg.REQUEST_TIMEOUT_S)
            api_url = parsing.select_online_api_url(page_url, html)
            payload = http.fetch_json(session, api_url, timeout=cfg.REQUEST_TIMEOUT_S)
            group, category, subcategory = parsing.labels_from_online_payload(payload, page_url)
        except Exception as exc:
            print(f"[WARN] online target discovery failed url={page_url}: {exc}")
            continue

        if api_url in seen:
            continue
        seen.add(api_url)
        targets.append(
            {
                "group": group,
                "category": category,
                "subcategory": subcategory,
                "url": api_url,
            }
        )
        if sleep_s > 0:
            time.sleep(sleep_s)

    return targets


def discover_leaflet_targets(session, index_url: str, max_leaflets: int | None, sleep_s: float) -> List[CategoryTarget]:
    """Discover weekly leaflet metadata targets.

    These targets are metadata-only for v1: scrape_daily skips group="leaflet".
    """
    print(f"[LEAFLET] discover index={index_url}")
    try:
        html = http.fetch_text(session, index_url, timeout=cfg.REQUEST_TIMEOUT_S)
    except Exception as exc:
        print(f"[WARN] leaflet index fetch failed url={index_url}: {exc}")
        return []

    urls = parsing.extract_leaflet_urls(html)
    if max_leaflets is not None:
        urls = urls[:max_leaflets]

    targets: List[CategoryTarget] = []
    seen: Set[str] = set()
    for leaflet_url in urls:
        identifier = parsing.leaflet_identifier_from_url(leaflet_url)
        if not identifier or leaflet_url in seen:
            continue
        seen.add(leaflet_url)

        category = "Folletos"
        subcategory = parsing.slug_to_label(identifier)
        api_url = parsing.leaflet_api_url(identifier)
        try:
            payload = http.fetch_json(session, api_url, timeout=cfg.REQUEST_TIMEOUT_S)
            category, subcategory = parsing.labels_from_leaflet_payload(payload, leaflet_url)
        except Exception as exc:
            print(f"[WARN] leaflet metadata fetch failed url={leaflet_url}: {exc}")

        targets.append(
            {
                "group": "leaflet",
                "category": category,
                "subcategory": subcategory,
                "url": leaflet_url,
            }
        )
        if sleep_s > 0:
            time.sleep(sleep_s)

    return targets


def discover_targets(max_groups: int | None, max_leaflets: int | None, sleep_s: float) -> List[CategoryTarget]:
    session = http.build_session()
    online_urls = cfg.ONLINE_CATEGORY_URLS
    if max_groups is not None:
        online_urls = online_urls[:max_groups]

    targets = []
    targets.extend(discover_online_targets(session, online_urls, sleep_s=sleep_s))
    targets.extend(
        discover_leaflet_targets(
            session,
            index_url=cfg.LEAFLET_INDEX_URL,
            max_leaflets=max_leaflets,
            sleep_s=sleep_s,
        )
    )

    deduped: List[CategoryTarget] = []
    seen: Set[Tuple[str, str, str, str]] = set()
    for target in targets:
        key = (target["group"], target["category"], target["subcategory"], target["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(target)

    deduped.sort(key=lambda r: (r["group"], r["category"], r["subcategory"], r["url"]))
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default="scrapers/lidl/targets_weekly/files/lidl_categories.json",
    )
    parser.add_argument("--sleep", type=float, default=cfg.REQUEST_SLEEP_S, help="Sleep between requests")
    parser.add_argument("--max-groups", type=int, default=None, help="Limit online category seed URLs")
    parser.add_argument("--max-leaflets", type=int, default=None, help="Limit leaflet metadata targets")
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    targets = discover_targets(
        max_groups=args.max_groups,
        max_leaflets=args.max_leaflets,
        sleep_s=args.sleep,
    )

    payload: Dict[str, object] = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_group_urls": cfg.ONLINE_CATEGORY_URLS + [cfg.LEAFLET_INDEX_URL],
        "count": len(targets),
        "targets": targets,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {len(targets)} targets -> {out_path}")

    object_name = "lidl/lidl_categories.json"
    if args.upload_to_gcs:
        gcs.validate_gcs_upload_config(str(out_path), cfg.GCS_BUCKET, object_name)
        try:
            uri = gcs.upload_file(
                local_path=str(out_path),
                bucket_name=cfg.GCS_BUCKET,
                object_name=object_name,
                object_type="application/json",
            )
            print(f"[UPLOAD] OK -> {uri}")
        except Exception as exc:
            raise RuntimeError(
                f"GCS upload failed (bucket={cfg.GCS_BUCKET}, object={object_name}, file={out_path})"
            ) from exc

    if args.upload_to_gcs and cfg.KEEP_LOCAL_FILES != "true":
        os.remove(out_path)
        print(f"[CLEANUP] deleted {out_path}")
    else:
        print("[CLEANUP] KEEP_LOCAL_FILES=true, skipping delete")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


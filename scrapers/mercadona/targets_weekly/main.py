#!/usr/bin/env python3
"""Build a reusable Mercadona category target file."""

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

from jobs.mercadona.common import config as cfg
from jobs.mercadona.common import gcs, http, parsing
from jobs.mercadona.common.models import CategoryTarget


def discover_targets() -> List[CategoryTarget]:
    """Discover category targets from Mercadona categories API."""
    session = http.build_session()
    payload = http.fetch_json(session, cfg.CATEGORIES_API_URL, timeout=cfg.REQUEST_TIMEOUT_S)

    targets: List[CategoryTarget] = []
    seen: Set[Tuple[str, str, str]] = set()
    for group in payload.get("results", []):
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("name") or "").strip()
        if not group_name:
            continue
        group_slug = parsing.slugify(group_name)
        for subcat in group.get("categories", []):
            if not isinstance(subcat, dict):
                continue
            if subcat.get("published") is False:
                continue
            subcat_id = subcat.get("id")
            subcat_name = str(subcat.get("name") or "").strip()
            if not subcat_id or not subcat_name:
                continue
            url = parsing.category_api_url(int(subcat_id))
            target: CategoryTarget = {
                "group": group_slug,
                "category": group_name,
                "subcategory": subcat_name,
                "url": url,
            }
            key = (target["category"], target["subcategory"], target["url"])
            if key in seen:
                continue
            seen.add(key)
            targets.append(target)

    targets.sort(key=lambda r: (r["group"], r["category"], r["subcategory"], r["url"]))
    return targets


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default="jobs/mercadona/targets_weekly/files/mercadona_categories.json",
    )
    parser.add_argument("--sleep", type=float, default=cfg.REQUEST_SLEEP_S, help="Sleep between API calls")
    parser.add_argument("--max-groups", type=int, default=None, help="Limit to first N top-level groups")
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    targets = discover_targets()
    if args.max_groups is not None:
        allowed_groups: List[str] = []
        seen_groups: Set[str] = set()
        for t in targets:
            g = (t.get("group") or "").strip()
            if not g or g in seen_groups:
                continue
            seen_groups.add(g)
            allowed_groups.append(g)
            if len(allowed_groups) >= args.max_groups:
                break
        allowed_set = set(allowed_groups)
        targets = [t for t in targets if (t.get("group") or "").strip() in allowed_set]

    payload: Dict[str, object] = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_group_urls": [cfg.CATEGORIES_API_URL],
        "count": len(targets),
        "targets": targets,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {len(targets)} targets -> {out_path}")

    object_name = "mercadona/mercadona_categories.json"
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

    if args.sleep > 0:
        time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

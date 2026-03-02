#!/usr/bin/env python3
"""Build a reusable Al Campo category/subcategory target file."""

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from jobs.al_campo.common import config as cfg
from jobs.al_campo.common import gcs, parsing, http
from jobs.al_campo.common.models import CategoryTarget


def discover_subcategories(
    session: requests.Session,
    group_slug: str,
    group_url: str,
    timeout_s: int,
    sleep_s: float,
) -> List[CategoryTarget]:
    results: List[CategoryTarget] = []
    html = http.fetch(session, group_url, timeout_s)
    soup = BeautifulSoup(html, "html.parser")

    group_label = cfg.GROUP_LABELS.get(group_slug, group_slug.replace("-", " ").title())

    links = parsing.extract_category_links(soup, group_slug)
    if not links:
        # No subcategory nav found — treat the group page itself as the target
        results.append(
            {
                "group": group_slug,
                "category": group_label,
                "subcategory": "",
                "url": group_url,
            }
        )
        return results

    for subcategory_name, url in links:
        time.sleep(sleep_s)
        results.append(
            {
                "group": group_slug,
                "category": group_label,
                "subcategory": subcategory_name,
                "url": url,
            }
        )

    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default="jobs/al_campo/targets_weekly/files/al_campo_categories.json",
    )
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--max-groups", type=int, default=None)
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    group_urls_env = os.environ.get("ALCAMPO_GROUP_URLS", "").strip()
    group_urls = (
        [u.strip() for u in group_urls_env.split(",") if u.strip()]
        if group_urls_env
        else cfg.DEFAULT_GROUP_URLS
    )

    if args.max_groups is not None:
        group_urls = group_urls[: args.max_groups]

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": cfg.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
    )
    retries = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    targets: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for group_url in group_urls:
        group_slug = parsing.group_slug_from_url(group_url)
        if not group_slug:
            print(f"[GROUP] skip invalid group url={group_url}")
            continue
        print(f"[GROUP] discover group={group_slug} url={group_url}")
        try:
            subcats = discover_subcategories(
                session=session,
                group_slug=group_slug,
                group_url=group_url,
                timeout_s=args.timeout,
                sleep_s=args.sleep,
            )
        except Exception as exc:
            print(f"[ERROR] discover failed group={group_slug}: {exc}")
            continue

        for target in subcats:
            key = (target["category"], target["subcategory"], target["url"])
            if key in seen:
                continue
            seen.add(key)
            targets.append(target)

        time.sleep(args.sleep)

    targets = sorted(
        targets,
        key=lambda r: (r["group"], r["category"], r["subcategory"], r["url"]),
    )

    payload = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_group_urls": group_urls,
        "count": len(targets),
        "targets": targets,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {len(targets)} targets -> {out_path}")

    object_name = "alcampo/al_campo_categories.json"

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

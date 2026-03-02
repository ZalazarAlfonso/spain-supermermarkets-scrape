#!/usr/bin/env python3
"""Build a reusable Carrefour category/subcategory target file."""

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from jobs.carrefour.common import config as cfg
from jobs.carrefour.common import gcs, parsing, http
from jobs.carrefour.common.models import CategoryTarget

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

    nav = soup.find("div", class_="nav-second-level-categories")
    if not nav:
        return results

    slides = nav.find_all("div", class_="nav-second-level-categories__slide")

    if group_slug == "bebidas":
        for slide in slides:
            a = slide.find("a", href=True)
            if not a:
                continue
            url = parsing.normalize_url(a.get("href"))
            if not url or not parsing.is_category_url(url):
                continue
            text = slide.get("title", "").strip()
            if not text:
                text_el = a.select_one("p.nav-second-level-categories__text")
                text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
            if not text or "oferta" in text.lower():
                continue
            results.append(
                {
                    "group":group_slug,
                    "category":"Bebidas",
                    "subcategory":text,
                    "url":url
                }
            )
        return results

    category_urls: Dict[str, str] = {}
    for slide in slides:
        a = slide.find("a", href=True)
        if not a:
            continue
        url = parsing.normalize_url(a.get("href"))
        if not url or not parsing.is_category_url(url):
            continue
        text = slide.get("title", "").strip()
        if not text:
            text_el = a.select_one("p.nav-second-level-categories__text")
            text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
        if not text or "oferta" in text.lower():
            continue
        if "oferta" in text.lower():
            continue
        if text not in category_urls:
            category_urls[text] = url

    for category_label, category_url in sorted(category_urls.items()):
        time.sleep(sleep_s)
        try:
            cat_html = http.fetch(session, category_url, timeout_s)
        except Exception:
            results.append(
                {
                    "group":group_slug,
                    "category":category_label, "subcategory":"", 
                    "url":category_url
                }
            )
            continue

        cat_soup = BeautifulSoup(cat_html, "html.parser")
        cat_nav = cat_soup.find("div", class_="nav-second-level-categories")
        if not cat_nav:
            results.append(
                {
                    "group":group_slug,
                    "category":category_label,
                    "subcategory": "", 
                    "url":category_url
                }
                )
            continue

        found_any = False
        for a in cat_nav.find_all("a", href=True):
            url = parsing.normalize_url(a.get("href"))
            if not url or not parsing.is_category_url(url):
                continue
            slide = a.find_parent(class_="nav-second-level-categories__slide")
            text = slide.get("title", "").strip() if slide else ""
            if not text:
                text_el = a.select_one("p.nav-second-level-categories__text")
                text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
            if not text:
                continue
            if "oferta" in text.lower():
                continue
            results.append(
                {
                    "group":group_slug,
                    "category":category_label,
                    "subcategory":text,
                    "url":url
                }
            )
            found_any = True

        if not found_any:
            results.append(
                {
                    "group":group_slug,
                    "category":category_label,
                    "subcategory": "", 
                    "url":category_url
                }
                )

    return results

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="files/carrefour_categories.json")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between category requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--max-categories", type=int, default=None)
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    group_urls_env = os.environ.get("CARREFOUR_GROUP_URLS", "").strip()
    group_urls = [u.strip() for u in group_urls_env.split(",") if u.strip()] if group_urls_env else cfg.DEFAULT_GROUP_URLS

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

    # Iterate over groups and extract targets.
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


    # Max categories filtering.
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

    # Organize targets.
    targets = sorted(
        targets,
        key=lambda r: (r["group"], r["category"], r["subcategory"], r["url"]),
    )

    # Final paylod build.
    payload = {
        "generated_at_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "source_group_urls": group_urls,
        "count": len(targets),
        "targets": targets,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {len(targets)} targets -> {out_path}")

    object_name = f"carrefour/carrefour_categories.json"

    # Upload to GCS.
    if args.upload_to_gcs:
        gcs.validate_gcs_upload_config(args.out, cfg.GCS_BUCKET, object_name)
        try:
            uri = gcs.upload_file(
                local_path=out_path,
                bucket_name=cfg.GCS_BUCKET,
                object_name=object_name,
                object_type="json"
            )
            print(f"[UPLOAD] OK -> {uri}")
        except Exception as exc:
            raise RuntimeError(
                f"GCS upload failed (bucket={cfg.GCS_BUCKET}, object={object_name}, file={out_path})"
            ) from exc

    # Clean up of the files
    if args.upload_to_gcs and cfg.KEEP_LOCAL_FILES != "true":
        os.remove(out_path)
        print(f"[CLEANUP] deleted {out_path}")
    else:
        print('[CLEANUP] KEEP_LOCAL_FILES=true, skipping delete')

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

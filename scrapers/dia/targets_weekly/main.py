#!/usr/bin/env python3
"""Build a reusable Dia category/subcategory target file."""

import argparse
import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import unquote, urlparse, urlunparse
import os
from scrapers.dia.common import config as cfg
from scrapers.dia.common import gcs, http, parsing
from scrapers.dia.common.models import CategoryTarget


DESKTOP_MENU_BUTTON_SELECTOR = "button[data-test-id='desktop-category-button']"
DESKTOP_CATEGORIES_LIST_SELECTOR = "ul[data-test-id='categories-list']"
DESKTOP_CATEGORY_ROW_LINK_SELECTOR = (
    "ul[data-test-id='categories-list'] > li a, "
    "ul[data-test-id='categories-list'] a[data-test-id='category-item-link']"
)
SUBCATEGORY_LIST_SELECTOR = "ul[data-test-id='sub-categories-list']"
SUBCATEGORY_ROW_LINK_SELECTOR = (
    "ul[data-test-id='sub-categories-list'] a, "
    "a[data-test-id='sub-category-item-link']"
)
SUBCATEGORY_TITLE_SELECTOR = "[data-test-id='sub-category-item-title']"
CATEGORY_ID_RE = re.compile(r"^L\d+$", flags=re.IGNORECASE)


def _clean_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def _split_path(url: str) -> List[str]:
    return [unquote(s) for s in urlparse(url).path.split("/") if s]


def _slug_from_path_segment(url: str, index: int) -> str:
    segs = _split_path(url)
    if 0 <= index < len(segs):
        return segs[index]
    return ""


def _is_dia_category_link(url: str) -> bool:
    segs = _split_path(url)
    return len(segs) == 3 and segs[1] == "c" and bool(CATEGORY_ID_RE.fullmatch(segs[2]))


def _is_dia_subcategory_link(url: str) -> bool:
    segs = _split_path(url)
    return len(segs) >= 4 and segs[-2] == "c" and bool(CATEGORY_ID_RE.fullmatch(segs[-1]))


def _discover_categories_from_home(timeout_s: int) -> List[Tuple[str, str]]:
    homepage_url = cfg.BASE_URL
    discovered: List[Tuple[str, str]] = []
    seen_urls: Set[str] = set()

    try:
        items = http.collect_interactive_links(
            url=homepage_url,
            click_selectors=[],
            click_chain=[
                {
                    "label": "accept-cookies",
                    "selector": "button#onetrust-accept-btn-handler",
                    "click": True,
                    "required": False,
                    "wait_ms": 3_000,
                },
                {
                    "label": "desktop-category-button",
                    "selector": DESKTOP_MENU_BUTTON_SELECTOR,
                    "click": True,
                    "required": True,
                    "wait_ms": 12_000,
                },
            ],
            link_selector=DESKTOP_CATEGORY_ROW_LINK_SELECTOR,
            title_selector="",
            wait_selector=DESKTOP_CATEGORIES_LIST_SELECTOR,
            timeout=timeout_s,
            mobile_viewport=False,
            max_scroll_steps=12,
        )
    except Exception as exc:
        print(f"[MENU][desktop-primary] failed url={homepage_url}: {exc}")
        return []

    for item in items:
        url = _clean_url(item.get("url", ""))
        if not url or url in seen_urls:
            continue
        if not _is_dia_category_link(url):
            continue

        title = (item.get("title") or "").strip()
        if not title:
            title = parsing.slug_to_label(_slug_from_path_segment(url, 0))
        if not title:
            continue

        seen_urls.add(url)
        discovered.append((title, url))

    print(f"[MENU][desktop-primary] categories total={len(discovered)} url={homepage_url}")

    return sorted(discovered, key=lambda r: (r[0].lower(), r[1]))


def _discover_subcategories(category_url: str, timeout_s: int) -> List[Tuple[str, str]]:
    try:
        items = http.collect_interactive_links(
            url=category_url,
            click_selectors=[],
            link_selector=SUBCATEGORY_ROW_LINK_SELECTOR,
            title_selector=SUBCATEGORY_TITLE_SELECTOR,
            wait_selector=SUBCATEGORY_LIST_SELECTOR,
            timeout=timeout_s,
            mobile_viewport=False,
            max_scroll_steps=30,
        )
        print(f"[CATEGORY][subcats] stage=desktop-list raw_items={len(items)} url={category_url}")
    except Exception as exc:
        print(f"[CATEGORY][subcats] stage=desktop-list failed url={category_url}: {exc}")
        return []

    category_root = _slug_from_path_segment(category_url, 0)
    out: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    for item in items:
        url = _clean_url(item.get("url", ""))
        if not url or url in seen or url == _clean_url(category_url):
            continue
        if not _is_dia_subcategory_link(url):
            continue
        if category_root and _slug_from_path_segment(url, 0) != category_root:
            continue

        title = (item.get("title") or "").strip()
        if not title:
            segs = _split_path(url)
            title = parsing.slug_to_label(segs[-3] if len(segs) >= 3 else "")
        if not title:
            continue

        seen.add(url)
        out.append((title, url))

    return sorted(out, key=lambda r: (r[0].lower(), r[1]))


def discover_targets(timeout_s: int, sleep_s: float) -> List[CategoryTarget]:
    """Discover Dia targets from homepage menu: categories -> subcategory routes."""
    print(f"[MENU] opening desktop categories menu on url={cfg.BASE_URL}")
    discovered_categories = _discover_categories_from_home(timeout_s=timeout_s)
    print(f"[MENU] found categories={len(discovered_categories)} url={cfg.BASE_URL}")

    results: List[CategoryTarget] = []
    seen_targets: Set[Tuple[str, str, str]] = set()

    for category_name, category_url in discovered_categories:
        group_slug = parsing.group_slug_from_url(category_url) or _slug_from_path_segment(category_url, 0) or (
            category_name.strip().lower().replace(" ", "-")
        )
        time.sleep(sleep_s)

        try:
            subcategories = _discover_subcategories(category_url=category_url, timeout_s=timeout_s)
            print(
                f"[CATEGORY][subcats] category='{category_name}' url={category_url} "
                f"subcategories={len(subcategories)}"
            )
        except Exception as exc:
            print(f"[WARN] skipping invalid category url={category_url}: {exc}")
            continue

        if not subcategories:
            key = (category_name, "", category_url)
            if key not in seen_targets:
                seen_targets.add(key)
                results.append(
                    {
                        "group": group_slug,
                        "category": category_name,
                        "subcategory": "",
                        "url": category_url,
                    }
                )
            continue

        for subcategory_name, subcategory_url in subcategories:
            key = (category_name, subcategory_name, subcategory_url)
            if key in seen_targets:
                continue
            seen_targets.add(key)
            results.append(
                {
                    "group": group_slug,
                    "category": category_name,
                    "subcategory": subcategory_name,
                    "url": subcategory_url,
                }
            )

    results.sort(key=lambda r: (r["group"], r["category"], r["subcategory"], r["url"]))
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="jobs/dia/targets_weekly/files/dia_categories.json")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--max-groups", type=int, default=None, help="Unused for DIA; kept for CLI compatibility")
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    if args.max_groups is not None:
        print("[INFO] --max-groups ignored for DIA; discovery always starts from homepage menu")

    targets = discover_targets(timeout_s=args.timeout, sleep_s=args.sleep)
    payload: Dict[str, object] = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_group_urls": [cfg.BASE_URL],
        "count": len(targets),
        "targets": targets,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] wrote {len(targets)} targets -> {out_path}")

    object_name = "dia/dia_categories.json"
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

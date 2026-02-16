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
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import random
from utils import gcp_bucket_uploader
from pathlib import Path


GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-eu")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

def validate_gcs_upload_config(local_path: str, bucket_name: str, object_name: str) -> None:
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Upload enabled but `GCS_BUCKET` is not set.")

    if not object_name or not object_name.strip():
        raise ValueError("Upload enabled but generated `object_name` is empty.")

    if not Path(local_path).is_file():
        raise FileNotFoundError(f"Local CSV not found: {local_path}")


try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None

BASE_URL = "https://www.carrefour.es"
SUPERMERCADO_URL = f"{BASE_URL}/supermercado"
DEFAULT_GROUP_URLS = [
    f"{BASE_URL}/supermercado/frescos/cat20002/c",
    f"{BASE_URL}/supermercado/la-despensa/cat20001/c",
    f"{BASE_URL}/supermercado/bebidas/cat20003/c",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

PRICE_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*,\d{2})\s*€")

OFFER_KEYWORDS = [
    "Promoción",
    "Promocion",
    "Oferta",
    "Descuento",
    "2ª unidad",
    "2a unidad",
    "Rebaja",
]

def normalize_url(href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return BASE_URL + href
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return None

def fetch_with_playwright(url: str, timeout: int = 40) -> str:
    """Fetch rendered HTML with Playwright (Chromium)."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required to bypass 403/JS rendering on carrefour.es. "
            "Install with: pip install playwright && playwright install"
        )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            locale="es-ES",
            user_agent=USER_AGENT,
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)

        # try to wait for either categories or product cards
        try:
            page.wait_for_selector("div.nav-second-level-categories, li.product-card-list__item, div.product-card", timeout=10_000)
        except Exception:
            pass

        html = page.content()
        context.close()
        browser.close()
        return html

def fetch(session: requests.Session, url: str, timeout: int = 30) -> str:
    """Fetch HTML; fall back to Playwright on 403 or if HTML looks incomplete."""
    resp = session.get(url, timeout=timeout, allow_redirects=True)

    # Carrefour often returns 403 to non-browser clients
    if resp.status_code == 403:
        return fetch_with_playwright(url, timeout=max(timeout, 40))

    resp.raise_for_status()
    html = resp.text

    # If page is JS-hydrated and key containers are missing, fallback.
    if ("nav-second-level-categories" not in html) and ("product-card" not in html):
        # Don't always fallback (can be normal), but for category/product pages it's helpful.
        return fetch_with_playwright(url, timeout=max(timeout, 40))

    return html

def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def extract_links(soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    for a in soup.find_all("a"):
        href = a.get("href")
        url = normalize_url(href)
        if url:
            links.append(url)
    return links


def is_category_url(url: str) -> bool:
    return "/c" in url #and "/p" not in url


def is_product_url(url: str) -> bool:
    return "/p" in url


def slug_to_title(slug: str) -> str:
    return slug.replace("-", " ").strip().title()


def group_slug_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    segs = [s for s in path.split("/") if s]
    # expected: supermercado/<group>/...
    if len(segs) >= 2 and segs[0] == "supermercado":
        return segs[1]
    return ""


def category_from_url(url: str, group_slug: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    segs = [s for s in path.split("/") if s]
    # expected: supermercado/<group>/<category>/...
    if len(segs) >= 3 and segs[0] == "supermercado" and segs[1] == group_slug:
        cat_slug = segs[2]
        # ignore group root and non-category slugs
        if cat_slug.lower().startswith("cat") or cat_slug.startswith("F-"):
            return ""
        return slug_to_title(cat_slug)
    return ""


def discover_subcategories(
    session: requests.Session, group_slug: str, group_url: str, sleep_s: float
) -> List[Tuple[str, str, str]]:
    """
    Return list of (category, subcategory, category_url) based on rules:

    - Frescos/La Despensa: categories come from the group page nav; subcategories come from each category page nav.
    - Bebidas: category = "Bebidas"; subcategory = group page nav text (iterate those links).
    """
    results: List[Tuple[str, str, str]] = []

    try:
        html = fetch(session, group_url)
        time.sleep(max(0.4, sleep_s * 0.6) + random.random() * 0.4)
        soup = soup_from_html(html)
    except Exception:
        return results

    nav = soup.find("div", class_="nav-second-level-categories")
    if not nav:
        return results

    group_slides = nav.find_all("div", class_="nav-second-level-categories__slide")
    print(f"[GROUP] {group_slug} -> {len(group_slides)} category entries")

    # Bebidas: group page entries are the subcategories
    if group_slug == "bebidas":
        for slide in group_slides:
            a = slide.find("a", href=True)
            if not a:
                continue
            url = normalize_url(a.get("href"))
            if not url or not is_category_url(url):
                continue
            text = ""
            if slide.get("title"):
                text = slide.get("title", "").strip()
            if not text:
                text_el = a.select_one("p.nav-second-level-categories__text")
                text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
            if not text:
                continue
            if "oferta" in text.lower():
                continue
            results.append(("Bebidas", text, url))
        time.sleep(sleep_s)
        return results

    # Frescos/La Despensa: group page entries are categories
    category_urls: Dict[str, str] = {}
    for slide in group_slides:
        a = slide.find("a", href=True)
        if not a:
            continue
        url = normalize_url(a.get("href"))
        if not url or not is_category_url(url):
            continue
        text = ""
        if slide.get("title"):
            text = slide.get("title", "").strip()
        if not text:
            text_el = a.select_one("p.nav-second-level-categories__text")
            text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
        if not text:
            continue
        if "oferta" in text.lower():
            continue
        if text not in category_urls:
            category_urls[text] = url

    time.sleep(sleep_s)

    for cat_label, cat_url in sorted(category_urls.items()):
        try:
            cat_html = fetch(session, cat_url)
            time.sleep(max(0.4, sleep_s * 0.6) + random.random() * 0.4)
            cat_soup = soup_from_html(cat_html)
        except Exception:
            continue

        cat_nav = cat_soup.find("div", class_="nav-second-level-categories")
        if not cat_nav:
            results.append((cat_label, "", cat_url))
            print(f"[CATEGORY] {cat_label} -> 0 subcategories (using category page)")
            time.sleep(sleep_s)
            continue

        subcat_links = cat_nav.find_all("a", href=True)
        print(f"[CATEGORY] {cat_label} -> {len(subcat_links)} subcategories")
        found_any = False
        for a in subcat_links:
            url = normalize_url(a.get("href"))
            if not url or not is_category_url(url):
                continue
            slide = a.find_parent(class_="nav-second-level-categories__slide")
            text = ""
            if slide and slide.get("title"):
                text = slide.get("title", "").strip()
            if not text:
                text_el = a.select_one("p.nav-second-level-categories__text")
                text = text_el.get_text(" ", strip=True) if text_el else a.get_text(" ", strip=True)
            if not text:
                continue
            results.append((cat_label, text, url))
            found_any = True

        if not found_any:
            results.append((cat_label, "", cat_url))

        time.sleep(sleep_s)

    return results


def extract_pagination_links(soup: BeautifulSoup, category_url: str) -> List[str]:
    links = extract_links(soup)
    base = category_url.split("?")[0]
    pages: Set[str] = set()

    # rel=next
    rel_next = soup.find_all("a", rel=lambda v: v and "next" in v)
    for a in rel_next:
        href = a.get("href")
        url = normalize_url(href)
        if url:
            pages.add(url)

    for link in links:
        if not is_category_url(link):
            continue
        if not link.startswith(base):
            continue
        # keep only links that look like pagination (query params or page indicators)
        low = link.lower()
        if "?" in link or "page" in low or "pag" in low or "no=" in low:
            pages.add(link)
    return sorted(pages)


def extract_product_links(soup: BeautifulSoup) -> List[str]:
    links = extract_links(soup)
    prods = [url for url in links if is_product_url(url)]
    # remove duplicates while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for url in prods:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def extract_product_links_from_html(html: str) -> List[str]:
    # Fallback regex-based extraction in case product links are not in standard hrefs
    patterns = [
        r"https?://www\\.carrefour\\.es/supermercado/[^\"'\\s>]+/R-[^\"'\\s>]+/p",
        r"/supermercado/[^\"'\\s>]+/R-[^\"'\\s>]+/p",
    ]
    found: List[str] = []
    for pat in patterns:
        for m in re.findall(pat, html, flags=re.IGNORECASE):
            url = m
            if url.startswith("/"):
                url = BASE_URL + url
            found.append(url)
    # de-dupe while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for url in found:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def extract_offer_flag(text: str) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in OFFER_KEYWORDS)


def parse_product_card(card: BeautifulSoup) -> Tuple[str, str, str, str, bool]:
    name = ""
    price = ""
    price_per_unit = ""
    brand = ""

    # Name
    title_link = card.select_one("h2.product-card__title a")
    if title_link:
        name = title_link.get_text(" ", strip=True)

    # Price + PPU (prefer data attributes on parent wrapper, then fall back to spans)
    parent = card.find_parent("div", class_="product-card__parent")
    if parent:
        price = (parent.get("app_price") or "").strip()
        price_per_unit = (parent.get("app_price_per_unit") or "").strip()

    if not price:
        price_el = card.select_one(".product-card__price")
        if price_el:
            price = price_el.get_text(" ", strip=True)

    if not price_per_unit:
        ppu_el = card.select_one(".product-card__price-per-unit")
        if ppu_el:
            price_per_unit = ppu_el.get_text(" ", strip=True)

    # Brand: Carrefour cards often omit explicit brand; try common spots if present
    brand_el = card.select_one(".product-card__brand, .product-card__brand-name, [data-testid='brand']")
    if brand_el:
        brand = brand_el.get_text(" ", strip=True)

    offer = extract_offer_flag(card.get_text(" ", strip=True))

    return name, brand, price, price_per_unit, offer


def scrape_category(
    session: requests.Session,
    category_label: str,
    subcategory_label: str,
    category_url: str,
    sleep_s: float,
    max_pages: Optional[int],
) -> List[Dict[str, str]]:
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
            page_html = fetch(session, page_url)
            time.sleep(max(0.4, sleep_s * 0.6) + random.random() * 0.4)
        except Exception:
            continue
        page_soup = soup_from_html(page_html)

        cards = page_soup.select("div.product-card")
        print(
            f"[PAGE] {category_label} | {subcategory_label or '-'} -> {page_url} "
            f"cards={len(cards)}"
        )
        if not cards:
            product_urls = extract_product_links(page_soup)
            if not product_urls:
                product_urls = extract_product_links_from_html(page_html)
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
                name, brand, price, price_per_unit, offer = parse_product_card(card)
                link = ""
                link_el = card.select_one("a.product-card__title-link") or card.select_one("a.product-card__media-link")
                if link_el:
                    link = normalize_url(link_el.get("href", "")) or ""

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

        for next_url in extract_pagination_links(page_soup, category_url):
            if next_url not in seen_pages and next_url not in page_queue:
                page_queue.append(next_url)
        print(f"[PAGINATION] {category_label} -> queued={len(page_queue)} seen={len(seen_pages)}")

        time.sleep(sleep_s)

    return rows


def main() -> int:
    start_time = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="files", help="Output directory")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests (seconds)")
    parser.add_argument("--max-categories", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--allow-duplicates", action="store_true", help="Allow duplicates across categories")
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload to Google Cloud Bucket")
    args = parser.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    today = dt.date.today().isoformat()
    filename = f"carrefour_supermercado_{today}.csv"
    out_path = os.path.join(out_dir, filename)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
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

    all_categories: List[Tuple[str, str, str]] = []
    group_urls = os.environ.get("CARREFOUR_GROUP_URLS", "")
    if group_urls.strip():
        group_list = [u.strip() for u in group_urls.split(",") if u.strip()]
    else:
        group_list = DEFAULT_GROUP_URLS

    for group_url in group_list:
        group_slug = group_slug_from_url(group_url)
        if not group_slug:
            print(f"[GROUP] Skipping invalid group URL: {group_url}")
            continue
        subcats = discover_subcategories(session, group_slug, group_url, args.sleep)
        all_categories.extend(subcats)
    
    print(f"[TARGETS] {len(all_categories)} category targets discovered")
    if args.max_categories is not None:
        all_categories = all_categories[: args.max_categories]
    
    # Error variables initialization.
    error_count = 0
    error_samples = []

    for category_label, subcategory_label, cat_url in all_categories:
        if args.max_products is not None and len(rows) >= args.max_products:
            break
        try:
            print(
                f"[SCRAPE] category='{category_label}' subcategory='{subcategory_label or '-'}' "
                f"url={cat_url}"
            )
            cat_rows = scrape_category(
                session, category_label, subcategory_label, cat_url, args.sleep, args.max_pages
            )
        except Exception as e:
            error_count += 1
            print(f"[ERROR] scrape_category failed url={cat_url} category={category_label}: {e}")
            continue

        print(f"Scraping {category_label} with {len(cat_rows)} products")
        for row in cat_rows:
            if args.max_products is not None and len(rows) >= args.max_products:
                break
            p_url = row.get("product_url", "")
            if not args.allow_duplicates and p_url and p_url in seen_products:
                continue
            if not args.allow_duplicates and p_url:
                seen_products.add(p_url)
            row["date"] = today
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

    # after CSV is written
    filename = os.path.basename(out_path)
    object_name = f"carrefour/{today}/{filename}"

    if args.upload_to_gcs:
        validate_gcs_upload_config(out_path, GCS_BUCKET, object_name)
        try:
            uri = gcp_bucket_uploader.upload_csv_file(
                local_path=out_path,
                bucket_name=GCS_BUCKET,
                object_name=object_name,
            )
            print(f"[UPLOAD] OK -> {uri}")
        except Exception as exc:
            raise RuntimeError(
                f"GCS upload failed (bucket={GCS_BUCKET}, object={object_name}, file={out_path})"
            ) from exc

    # Clean up of the files
    if args.upload_to_gcs and os.getenv("KEEP_LOCAL_FILES", "false").lower() != "true":
        os.remove(out_path)
        print(f"[CLEANUP] deleted {out_path}")
    else:
        print('[CLEANUP] KEEP_LOCAL_FILES=true, skipping delete')

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"Wrote {len(rows)} rows to {out_path}. Time : {(elapsed_time)/60} minutes")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# jobs/carrefour/common/parsing.py
from typing import Dict, List, Optional, Set, Tuple
from .config import BASE_URL
from bs4 import BeautifulSoup
from .config import OFFER_KEYWORDS
import re
from urllib.parse import urlparse


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

def group_slug_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    segs = [s for s in path.split("/") if s]
    if len(segs) >= 2 and segs[0] == "supermercado":
        return segs[1]
    return ""

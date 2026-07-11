"""Parsing and normalization helpers for Lidl Spain payloads."""

import html
import json
import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

from . import config as cfg


LEAFLET_URL_RE = re.compile(r"https?://www\.lidl\.es/l/folletos/([^\"'\s<>]+?/ar/\d+)")
RELATIVE_LEAFLET_RE = re.compile(r"/l/folletos/([^\"'\s<>]+?/ar/\d+)")
API_URL_RE = re.compile(r"(?:https://www\.lidl\.es)?(/q/api/category/[^\"'\\<>\s]+)")
PRICE_PER_UNIT_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{1,3})?\s*€\s*(?:/|por)\s*[\w%]+)",
    flags=re.IGNORECASE,
)


def soup_from_html(raw_html: str) -> BeautifulSoup:
    return BeautifulSoup(raw_html, "html.parser")


def normalize_url(href: str, base_url: str = cfg.BASE_URL) -> Optional[str]:
    if not href:
        return None
    href = html.unescape(href).strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return urljoin(base_url, href)
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return None


def clean_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(fragment=""))


def strip_query(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text).strip("-").lower()
    return slug or "lidl"


def slug_to_label(slug: str) -> str:
    text = unquote(slug or "").strip().strip("/")
    if not text:
        return ""
    text = re.sub(r"^[chs](?=\d+$)", "", text)
    text = text.replace("-", " ").replace("_", " ")
    return " ".join(part.capitalize() for part in text.split())


def online_path_parts(url: str) -> Tuple[str, str, str]:
    """Return (type, slug, id) for Lidl online category/content paths."""
    path = urlparse(url).path
    parts = [unquote(p) for p in path.split("/") if p]
    if len(parts) >= 3 and parts[0] in {"c", "h"}:
        return parts[0], parts[1], parts[2]
    return "", "", ""


def group_slug_from_url(url: str) -> str:
    kind, slug, _ = online_path_parts(url)
    if kind and slug:
        return slugify(slug)
    return ""


def extract_online_api_urls(raw_html: str) -> List[str]:
    """Extract Lidl /q/api/category URLs embedded in Nuxt-rendered pages."""
    found: List[str] = []
    seen = set()
    for raw in API_URL_RE.findall(raw_html or ""):
        path = html.unescape(raw).replace("\\u002F", "/")
        path = path.replace("\\/", "/")
        url = normalize_url(path)
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        found.append(url)
    return found


def fallback_online_api_url(page_url: str) -> str:
    """Build a best-effort Lidl category API URL from a page URL."""
    kind, slug, item_id = online_path_parts(page_url)
    if not kind or not slug or not item_id:
        raise ValueError(f"Unable to build Lidl API URL from page URL: {page_url}")

    numeric_id = re.sub(r"^[a-zA-Z]", "", item_id)
    path = f"/q/api/category/{kind}/{quote(slug)}/{quote(item_id)}"
    query = urlencode(
        {
            "assortment": cfg.ONLINE_ASSORTMENT,
            "locale": cfg.ONLINE_LOCALE,
            "version": cfg.ONLINE_API_VERSION,
            "pageId": numeric_id,
        }
    )
    return f"{cfg.BASE_URL}{path}?{query}"


def select_online_api_url(page_url: str, raw_html: str) -> str:
    """Choose the most relevant embedded Lidl API URL for a page."""
    kind, slug, item_id = online_path_parts(page_url)
    candidates = extract_online_api_urls(raw_html)
    for url in candidates:
        path = unquote(urlparse(url).path)
        if f"/q/api/category/{kind}/{slug}/{item_id}" in path:
            return url
    if candidates:
        return candidates[0]
    return fallback_online_api_url(page_url)


def paged_api_url(api_url: str, offset: int, fetch_size: int) -> str:
    parsed = urlparse(api_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params["offset"] = str(offset)
    if "fetchsize" not in {k.lower() for k in params}:
        params["fetchsize"] = str(fetch_size)
    return urlunparse(parsed._replace(query=urlencode(params)))


def extract_leaflet_urls(raw_html: str) -> List[str]:
    """Return official Lidl leaflet URLs from a folletos page."""
    seen = set()
    urls: List[str] = []
    for match in LEAFLET_URL_RE.findall(raw_html or ""):
        url = f"{cfg.BASE_URL}/l/folletos/{match}"
        if url not in seen:
            seen.add(url)
            urls.append(url)
    for match in RELATIVE_LEAFLET_RE.findall(raw_html or ""):
        url = f"{cfg.BASE_URL}/l/folletos/{match}"
        if url not in seen:
            seen.add(url)
            urls.append(url)

    soup = soup_from_html(raw_html)
    for a in soup.find_all("a", href=True):
        href = normalize_url(a.get("href", ""))
        if not href or "/l/folletos/" not in href or "/ar/" not in href:
            continue
        url = strip_query(href)
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def leaflet_identifier_from_url(url: str) -> str:
    parts = [unquote(p) for p in urlparse(url).path.split("/") if p]
    if len(parts) >= 4 and parts[0] == "l" and parts[1] == "folletos":
        return parts[2]
    return ""


def leaflet_api_url(identifier: str) -> str:
    return f"{cfg.LEAFLET_API_BASE_URL}/flyer?{urlencode({'flyer_identifier': identifier})}"


def labels_from_online_payload(payload: Dict[str, Any], source_url: str) -> Tuple[str, str, str]:
    """Infer target group/category/subcategory from Lidl category payload."""
    details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
    category_data = details.get("categoryData") if isinstance(details.get("categoryData"), dict) else {}
    breadcrumbs = payload.get("breadcrumbs") if isinstance(payload.get("breadcrumbs"), list) else []

    labels: List[str] = []
    for crumb in breadcrumbs:
        if isinstance(crumb, dict):
            label = str(crumb.get("label") or crumb.get("name") or "").strip()
            if label:
                labels.append(label)

    category = str(category_data.get("name") or category_data.get("label") or "").strip()
    if not category and labels:
        category = labels[0]

    subcategory = ""
    if len(labels) > 1:
        subcategory = labels[-1]
    if not subcategory:
        subcategory = str(category_data.get("title") or "").strip()

    if not category:
        _, slug, _ = online_path_parts(source_url)
        category = slug_to_label(slug) or "Online"
    if subcategory and subcategory.lower() == category.lower():
        subcategory = ""

    group = group_slug_from_url(source_url) or slugify(category)
    return group, category, subcategory


def labels_from_leaflet_payload(payload: Dict[str, Any], fallback_url: str) -> Tuple[str, str]:
    flyer = payload.get("flyer") if isinstance(payload.get("flyer"), dict) else {}
    category = str(flyer.get("category") or "Folletos").strip()
    bits = [
        str(flyer.get("subcategory") or "").strip(),
        str(flyer.get("name") or "").strip(),
        str(flyer.get("title") or "").strip(),
    ]
    subcategory = " / ".join(bit for bit in bits if bit)
    if not subcategory:
        subcategory = slug_to_label(leaflet_identifier_from_url(fallback_url))
    return category or "Folletos", subcategory


def iter_product_items(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    items = payload.get("items") or []
    if not isinstance(items, list):
        return []
    return (item for item in items if isinstance(item, dict))


def product_data_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    gridbox = item.get("gridbox") if isinstance(item.get("gridbox"), dict) else {}
    data = gridbox.get("data") if isinstance(gridbox.get("data"), dict) else {}
    return data


def _format_price(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value).strip()


def _find_price_per_unit(value: Any) -> str:
    if isinstance(value, str):
        m = PRICE_PER_UNIT_RE.search(value.replace("\xa0", " "))
        return m.group(1).strip() if m else ""
    if isinstance(value, dict):
        for key in ("basePrice", "basicPrice", "pricePerUnit", "unitPrice", "referencePrice"):
            found = _find_price_per_unit(value.get(key))
            if found:
                return found
        for child in value.values():
            found = _find_price_per_unit(child)
            if found:
                return found
    if isinstance(value, list):
        for child in value:
            found = _find_price_per_unit(child)
            if found:
                return found
    return ""


def _offer_from_product(data: Dict[str, Any]) -> bool:
    price = data.get("price") if isinstance(data.get("price"), dict) else {}
    deal = data.get("dealOfDay") if isinstance(data.get("dealOfDay"), dict) else {}
    ribbons = data.get("ribbons") if isinstance(data.get("ribbons"), list) else []

    if deal.get("active") is True or data.get("flashSales") is True:
        return True
    if data.get("lidlPlus"):
        return True
    for key in ("oldPrice", "strokePrice", "strikethroughPrice", "recommendedRetailPrice", "discount"):
        if data.get(key) or price.get(key):
            return True
    for ribbon in ribbons:
        if not isinstance(ribbon, dict):
            continue
        text = " ".join(str(v) for v in ribbon.values()).lower()
        if any(word in text for word in ("oferta", "descuento", "rebaja", "promo")):
            return True
    return False


def product_row_from_item(item: Dict[str, Any], category: str, subcategory: str) -> Optional[Dict[str, str]]:
    data = product_data_from_item(item)
    if not data:
        return None

    product = str(data.get("fullTitle") or data.get("title") or "").strip()
    brand_data = data.get("brand") if isinstance(data.get("brand"), dict) else {}
    brand = str(brand_data.get("name") or "").strip()
    price_data = data.get("price") if isinstance(data.get("price"), dict) else {}
    price = _format_price(price_data.get("price"))
    price_per_unit = _find_price_per_unit(price_data) or _find_price_per_unit(data.get("keyfacts"))

    product_url = normalize_url(str(data.get("canonicalUrl") or data.get("canonicalPath") or "")) or ""
    if not product_url:
        product_id = str(data.get("productId") or data.get("erpNumber") or item.get("code") or "").strip()
        if product_id:
            product_url = f"{cfg.BASE_URL}/p/p{product_id}"

    if not product and not product_url:
        return None

    return {
        "product": product,
        "brand": brand,
        "price": price,
        "price_per_unit": price_per_unit,
        "offer": "true" if _offer_from_product(data) else "false",
        "category": category,
        "subcategory": subcategory,
        "product_url": product_url,
    }


def decode_grid_data(raw: str) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        data = json.loads(html.unescape(raw))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


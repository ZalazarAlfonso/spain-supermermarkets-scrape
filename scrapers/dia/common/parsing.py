import json
import re
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse, urlunparse

from bs4 import BeautifulSoup

from .config import BASE_URL, OFFER_KEYWORDS


PRICE_RE = re.compile(r"(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})\s*€")
PPU_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{3})*[.,]\d{2}\s*€\s*(?:/|por)\s*[\wáéíóúñ%]+)",
    flags=re.IGNORECASE,
)


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


def _normalize_for_match(text: str) -> str:
    s = (text or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _clean_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def slug_to_label(slug: str) -> str:
    text = (slug or "").strip().strip("/")
    text = unquote(text)
    if not text:
        return ""
    text = text.replace("-", " ").replace("_", " ")
    return " ".join(t.capitalize() for t in text.split())


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def group_slug_from_url(url: str) -> str:
    try:
        segs = [unquote(s) for s in urlparse(url).path.split("/") if s]
    except Exception:
        return ""

    # Typical Dia route: /compra-online/{group}
    if "compra-online" in segs:
        idx = segs.index("compra-online")
        if idx + 1 < len(segs):
            return segs[idx + 1]
        return "compra-online"

    # Dia category pattern often looks like /frutas/c/L105.
    if "c" in segs:
        idx = segs.index("c")
        if idx - 1 >= 0:
            return segs[idx - 1]
        if idx + 1 < len(segs):
            return segs[idx + 1]

    # Otherwise fallback to last path segment.
    return segs[-1] if segs else ""


def is_product_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    if parsed.netloc and "dia.es" not in parsed.netloc:
        return False
    return any(k in path for k in ("/p/", "/producto/", "/product/", "/products/", "/sku/"))


def _is_category_url(url: str) -> bool:
    if not url or is_product_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    if parsed.netloc and "dia.es" not in parsed.netloc:
        return False
    if "/compra-online" in path:
        return True
    return any(k in path for k in ("/categoria", "/categorias", "/category", "/c/", "/supermercado"))


def _ignore_nav_link(text: str, url: str) -> bool:
    t = _normalize_for_match(text)
    u = _normalize_for_match(url)
    deny = (
        "oferta",
        "promocion",
        "cupon",
        "login",
        "registro",
        "tiendas",
        "ayuda",
        "contacto",
        "carrito",
        "politica",
        "privacidad",
        "terminos",
    )
    return any(k in t or k in u for k in deny)


def extract_category_links(
    soup: BeautifulSoup,
    group_slug: str,
    group_url: str,
) -> List[Tuple[str, str]]:
    wanted_group = _normalize_for_match(group_slug)
    group_url_clean = _clean_url(group_url)

    seen_urls: Set[str] = set()
    results: List[Tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        abs_url = normalize_url(a.get("href", ""))
        if not abs_url:
            continue

        clean_url = _clean_url(abs_url)
        if clean_url == group_url_clean:
            continue
        if clean_url in seen_urls:
            continue
        if not _is_category_url(clean_url):
            continue

        text = a.get_text(" ", strip=True)
        if _ignore_nav_link(text, clean_url):
            continue

        # If we know a group slug, prioritize links that stay in that group branch.
        norm_url = _normalize_for_match(clean_url)
        if wanted_group and wanted_group not in {"", "compra online", "compra-online"}:
            if wanted_group not in norm_url and wanted_group not in _normalize_for_match(text):
                continue

        seen_urls.add(clean_url)
        name = text or slug_to_label(urlparse(clean_url).path.split("/")[-1])
        if not name:
            continue
        results.append((name, clean_url))

    return sorted(results, key=lambda x: (x[0].lower(), x[1]))


def extract_offer_flag(text: str) -> bool:
    t = _normalize_for_match(text)
    return any(k in t for k in OFFER_KEYWORDS)


def extract_product_cards(soup: BeautifulSoup) -> List[BeautifulSoup]:
    selectors = [
        "article[class*='product']",
        "div[class*='product-card']",
        "li[class*='product']",
        "[data-testid*='product']",
        "a[href*='/p/']",
        "a[href*='/producto/']",
        "a[href*='/product/']",
        "a[href*='/products/']",
    ]

    seen: Set[str] = set()
    cards: List[BeautifulSoup] = []

    for selector in selectors:
        for node in soup.select(selector):
            product_url = extract_product_url(node)
            if not product_url:
                continue
            if product_url in seen:
                continue
            seen.add(product_url)
            cards.append(node)
        if cards:
            break

    return cards


def extract_product_url(card: BeautifulSoup) -> str:
    href = (card.get("href") or "").strip()
    if href:
        normalized = normalize_url(href) or ""
        cleaned = _clean_url(normalized)
        if is_product_url(cleaned):
            return cleaned

    link = card.select_one("a[href]")
    if link:
        normalized = normalize_url(link.get("href", "")) or ""
        cleaned = _clean_url(normalized)
        if is_product_url(cleaned):
            return cleaned

    return ""


def detect_page_markers(soup: BeautifulSoup) -> Dict[str, bool]:
    txt = _normalize_for_match(soup.get_text(" ", strip=True))
    return {
        "location_gate": any(
            k in txt
            for k in (
                "codigo postal",
                "elige tu tienda",
                "selecciona tu tienda",
                "introduce tu codigo",
            )
        ),
        "empty_results": any(
            k in txt
            for k in (
                "no hay resultados",
                "sin resultados",
                "no se han encontrado productos",
            )
        ),
        "blocked": any(
            k in txt
            for k in (
                "access denied",
                "request blocked",
                "captcha",
                "checking your browser",
            )
        ),
    }


def _price_from_text(text: str) -> str:
    m = PRICE_RE.search(text)
    return m.group(1).strip() if m else ""


def _ppu_from_text(text: str) -> str:
    m = PPU_RE.search(text)
    if not m:
        return ""
    return m.group(1).replace("\xa0", " ").strip().replace("€/", "€ / ")


def parse_product_card(card: BeautifulSoup) -> Tuple[str, str, str, str, bool]:
    name = ""
    brand = ""
    price = ""
    price_per_unit = ""

    for selector in (
        "[data-testid*='name']",
        "[class*='product-name']",
        "[class*='name']",
        "h2",
        "h3",
        "h4",
    ):
        el = card.select_one(selector)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                name = txt
                break

    if not name:
        img = card.select_one("img[alt]")
        if img:
            name = (img.get("alt") or "").strip()

    brand_el = card.select_one("[class*='brand'], [data-testid*='brand']")
    if brand_el:
        brand = brand_el.get_text(" ", strip=True)

    if not brand and name:
        first = name.split(" ", 1)[0].strip(".,()")
        if len(first) > 1 and first.upper() == first:
            brand = first

    for selector in (
        "[data-testid*='price']",
        "[class*='price']",
        "strong",
        "span",
    ):
        for el in card.select(selector):
            txt = el.get_text(" ", strip=True)
            if "€" in txt:
                price = _price_from_text(txt) or txt.replace("€", "").strip()
                if not price_per_unit:
                    price_per_unit = _ppu_from_text(txt)
                if price:
                    break
        if price:
            break

    card_text = card.get_text(" ", strip=True)
    if not price:
        price = _price_from_text(card_text)
    if not price_per_unit:
        price_per_unit = _ppu_from_text(card_text)

    offer = extract_offer_flag(card_text)
    return name, brand, price, price_per_unit, offer


def _iter_ld_json_objects(raw: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        out.append(raw)
        graph = raw.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                out.extend(_iter_ld_json_objects(item))
        items = raw.get("itemListElement")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("item"), dict):
                    out.extend(_iter_ld_json_objects(item["item"]))
    elif isinstance(raw, list):
        for item in raw:
            out.extend(_iter_ld_json_objects(item))
    return out


def extract_products_from_json_ld(soup: BeautifulSoup) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    seen: Set[str] = set()

    for script in soup.select("script[type='application/ld+json']"):
        text = (script.string or script.get_text("", strip=True) or "").strip()
        if not text:
            continue
        try:
            raw = json.loads(text)
        except Exception:
            continue

        for obj in _iter_ld_json_objects(raw):
            if not isinstance(obj, dict):
                continue
            obj_type = _normalize_for_match(str(obj.get("@type") or ""))
            if "product" not in obj_type and "name" not in obj:
                continue

            name = str(obj.get("name") or "").strip()
            if not name:
                continue

            brand = ""
            b = obj.get("brand")
            if isinstance(b, dict):
                brand = str(b.get("name") or "").strip()
            elif isinstance(b, str):
                brand = b.strip()

            offers = obj.get("offers")
            price = ""
            price_per_unit = ""
            if isinstance(offers, dict):
                price = str(offers.get("price") or "").strip()
                unit_text = str(offers.get("priceSpecification") or "")
                price_per_unit = _ppu_from_text(unit_text)
            elif isinstance(offers, list) and offers:
                first = offers[0]
                if isinstance(first, dict):
                    price = str(first.get("price") or "").strip()

            url = normalize_url(str(obj.get("url") or "")) or ""
            if url:
                url = _clean_url(url)

            key = url or name.lower()
            if key in seen:
                continue
            seen.add(key)

            products.append(
                {
                    "product": name,
                    "brand": brand,
                    "price": price,
                    "price_per_unit": price_per_unit,
                    "offer": "false",
                    "product_url": url,
                }
            )

    return products

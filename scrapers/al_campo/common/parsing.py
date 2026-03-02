# jobs/al_campo/common/parsing.py
import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse, urljoin, urlunparse

from bs4 import BeautifulSoup

from .config import BASE_URL, OFFER_KEYWORDS


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


def is_product_url(url: str) -> bool:
    if not url or "/products/" not in url:
        return False
    parsed = urlparse(url)
    if parsed.netloc and "compraonline.alcampo.es" not in parsed.netloc:
        return False
    return True


def group_slug_from_url(url: str) -> str:
    """Extract the group slug from a category URL.

    e.g. https://www.compraonline.alcampo.es/categories/frescos/OC2112 -> 'frescos'
    """
    try:
        path = urlparse(url).path
    except Exception:
        return ""
    segs = [s for s in path.split("/") if s]
    if len(segs) >= 2 and segs[0] == "categories":
        return unquote(segs[1])
    return ""


def _normalize_slug_for_match(slug: str) -> str:
    """Normalize category/group slug for accent-insensitive matching."""
    if not slug:
        return ""
    s = unquote(slug).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def _strip_query(url: str) -> str:
    """Remove query string from URL."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def extract_category_links(soup: BeautifulSoup, group_slug: str) -> List[Tuple[str, str]]:
    """Return [(subcategory_name, absolute_url), ...] for navigation links under group_slug.

    Finds all <a href="/categories/{group_slug}/..."> links, strips query params,
    deduplicates by URL, and excludes the group page itself.
    """
    wanted_group = _normalize_slug_for_match(group_slug)
    seen_urls: Set[str] = set()
    results: List[Tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        abs_url = normalize_url(href)
        if not abs_url:
            continue

        # Strip query string and parse path segments.
        clean_href = _strip_query(abs_url)
        path = urlparse(clean_href).path
        path_segs = [unquote(s) for s in path.split("/") if s]
        # path_segs: ["categories", group_slug, ...]
        if len(path_segs) < 3 or path_segs[0] != "categories":
            continue

        current_group = _normalize_slug_for_match(path_segs[1])
        if current_group != wanted_group:
            continue

        if clean_href in seen_urls:
            continue
        seen_urls.add(clean_href)

        name = a.get_text(" ", strip=True)
        if not name:
            name = path_segs[-1].replace("-", " ").title()

        results.append((name, clean_href))

    return results


def extract_offer_flag(text: str) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in OFFER_KEYWORDS)


def extract_product_cards(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Return product card nodes using multiple selectors to survive DOM changes."""
    selectors = [
        "div.product-card-container",
        "a.product-card-container",
        "[data-test='fop-body']",
        "a[href*='/products/'][class*='product']",
        "[data-testid*='product'] a[href*='/products/']",
        "article a[href*='/products/']",
        "li a[href*='/products/']",
        "a[href*='/products/']",
    ]
    seen: Set[str] = set()
    cards: List[BeautifulSoup] = []

    for selector in selectors:
        for node in soup.select(selector):
            normalized = extract_product_url(node)
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            cards.append(node)
        if cards:
            # Prefer first selector that yields results to limit noise.
            break

    return cards


def extract_product_url(card: BeautifulSoup) -> str:
    """Extract canonical product URL from the card node."""
    href = (card.get("href") or "").strip()
    if href:
        normalized = normalize_url(href) or ""
        if normalized and is_product_url(normalized):
            return normalized

    link = card.select_one("a[href*='/products/']")
    if link:
        normalized = normalize_url(link.get("href", "")) or ""
        if normalized and is_product_url(normalized):
            return normalized

    return ""


def detect_page_markers(soup: BeautifulSoup) -> Dict[str, bool]:
    """Detect common non-product states: location gate, empty results, antibot."""
    txt = soup.get_text(" ", strip=True).lower()
    return {
        "location_gate": any(
            k in txt
            for k in (
                "código postal",
                "codigo postal",
                "elige tu tienda",
                "selecciona tu tienda",
                "introduce tu código",
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


def parse_product_card(card: BeautifulSoup) -> Tuple[str, str, str, str, bool]:
    """Parse a single <a class="product-card-container"> element.

    Returns:
        (name, brand, price, price_per_unit, offer)
    """
    name = ""
    price = ""
    price_per_unit = ""
    brand = ""

    # Name: prefer known selectors, then generic heading/text fallbacks
    title_h3 = card.select_one(".title-container h3")
    if title_h3:
        name = title_h3.get_text(" ", strip=True)
    if not name:
        title_a = card.select_one(".title-container a")
        if title_a:
            name = title_a.get_text(" ", strip=True)
    if not name:
        for selector in ("h1", "h2", "h3", "h4", "[class*='title']", "[class*='name']"):
            el = card.select_one(selector)
            if el:
                text = el.get_text(" ", strip=True)
                if text:
                    name = text
                    break
    if not name:
        img = card.select_one("img[alt]")
        if img:
            name = (img.get("alt") or "").strip()

    # Brand heuristic: first uppercase token in product name (e.g., "ALPRO ...")
    if name:
        first_token = name.split(" ", 1)[0].strip(".,()")
        if len(first_token) > 1 and first_token.upper() == first_token:
            brand = first_token

    # Price: current Alcampo cards use data-test="fop-price".
    price_el = card.select_one("[data-test='fop-price']")
    if price_el:
        price = price_el.get_text(" ", strip=True)

    # Legacy selector fallback.
    price_el = card.select_one(".price-container span.current-price")
    if not price and price_el:
        price = price_el.get_text(" ", strip=True)
    if not price:
        for selector in ("[class*='price']", "[data-testid*='price']", "strong", "span"):
            for el in card.select(selector):
                text = el.get_text(" ", strip=True)
                if "€" in text:
                    price = text
                    break
            if price:
                break
    if price:
        price = price.replace("\xa0", " ").replace("€", "").strip()

    # Price per unit: current selector first, then legacy and regex fallback.
    ppu_el = card.select_one("[data-test='fop-price-per-unit']")
    if ppu_el:
        price_per_unit = ppu_el.get_text(" ", strip=True)

    # Legacy selector fallback.
    ppu_container = card.select_one(".price-pack-size-container")
    if not price_per_unit and ppu_container:
        for span in ppu_container.find_all("span"):
            text = span.get_text(" ", strip=True)
            if "€" in text and "por" in text.lower():
                # Strip surrounding parentheses if present: "(1,39 € por kilogramo)" → "1,39 € por kilogramo"
                price_per_unit = text.strip("() ")
                break
    if not price_per_unit:
        card_text = card.get_text(" ", strip=True)
        # Examples: "1,39 € por kilogramo", "8,95 €/kg", "4,50 €/l"
        m = re.search(r"(\d+[.,]\d+\s*€\s*(?:/|por)\s*[\wáéíóúñ]+)", card_text, flags=re.IGNORECASE)
        if m:
            price_per_unit = m.group(1).strip()
    if price_per_unit:
        price_per_unit = price_per_unit.replace("\xa0", " ").strip("() ").replace("€/","€ / ")

    # Offer: presence of a promotion link inside .promotion-container
    offer_link = card.select_one(".promotion-container a[href*='/offers/']")
    if offer_link:
        offer = True
    else:
        offer = extract_offer_flag(card.get_text(" ", strip=True))

    return name, brand, price, price_per_unit, offer

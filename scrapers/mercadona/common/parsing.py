"""
Parsing and normalization helpers for Mercadona API payloads.
"""

import re
import unicodedata
from typing import Any, Dict

from . import config as cfg


def slugify(text: str) -> str:
    """Convert free text to a stable lowercase slug."""
    normalized = unicodedata.normalize("NFKD", text or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text).strip("-").lower()
    return slug or "mercadona"


def category_api_url(category_id: int) -> str:
    """Build category detail URL."""
    return cfg.CATEGORY_API_URL_TEMPLATE.format(category_id=category_id)


def category_id_from_url(url: str) -> int:
    """Extract category id from /api/categories/{id}/ URL."""
    m = re.search(r"/api/categories/(\d+)/?$", url or "")
    if not m:
        raise ValueError(f"Unable to parse Mercadona category id from url={url}")
    return int(m.group(1))


def extract_brand(product: Dict[str, Any]) -> str:
    """Extract brand from product payload using best-effort heuristics."""
    direct = str(product.get("brand") or "").strip()
    if direct:
        return direct

    name = str(product.get("display_name") or "").strip()
    low_name = name.lower()
    for brand in cfg.KNOWN_BRANDS:
        if brand.lower() in low_name:
            return brand

    if name:
        first = name.split(" ", 1)[0].strip(".,()")
        if len(first) > 1 and first.upper() == first:
            return first

        # Fallback: trailing title-case tokens often encode the brand.
        tokens = re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9ºª'-]+", name)
        trailing: list[str] = []
        for tok in reversed(tokens):
            if not tok:
                continue
            if tok[0].isupper() and not any(ch.isdigit() for ch in tok):
                trailing.append(tok)
                if len(trailing) >= 3:
                    break
                continue
            # Stop at first lowercase token boundary to avoid crossing words
            # like "Refresco Fanta limón" -> "Refresco Fanta".
            break
        if trailing:
            return " ".join(reversed(trailing))

    return ""


def extract_price_fields(product: Dict[str, Any]) -> tuple[str, str, bool]:
    """Extract price, price-per-unit and offer flag from price_instructions."""
    p = product.get("price_instructions") or {}
    if not isinstance(p, dict):
        p = {}

    price = str(p.get("bulk_price") or "").strip()

    price_per_unit = ""
    ref_price = str(p.get("reference_price") or "").strip()
    ref_fmt = str(p.get("reference_format") or "").strip()
    if ref_price and ref_fmt:
        price_per_unit = f"{ref_price} €/{ref_fmt}"
    else:
        unit_price = str(p.get("unit_price") or "").strip()
        size_fmt = str(p.get("size_format") or "").strip()
        if unit_price and size_fmt:
            price_per_unit = f"{unit_price} €/{size_fmt}"

    offer = bool(p.get("price_decreased")) or bool(p.get("previous_unit_price"))
    return price, price_per_unit, offer


def extract_product_url(product: Dict[str, Any]) -> str:
    """Return a canonical product URL."""
    share_url = str(product.get("share_url") or "").strip()
    if share_url:
        return share_url

    product_id = str(product.get("id") or "").strip()
    slug = str(product.get("slug") or "").strip()
    if product_id and slug:
        return f"{cfg.BASE_URL}/product/{product_id}/{slug}"
    if product_id:
        return f"{cfg.BASE_URL}/product/{product_id}"
    return ""


def build_subcategory_label(target_subcategory: str, leaf_name: str) -> str:
    """Compose a stable subcategory name from target + leaf bucket names."""
    target = (target_subcategory or "").strip()
    leaf = (leaf_name or "").strip()
    if leaf and target and leaf.lower() != target.lower():
        return f"{target} / {leaf}"
    return leaf or target

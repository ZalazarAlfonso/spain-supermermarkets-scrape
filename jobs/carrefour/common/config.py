"""
Configuration settings and environment variables.
"""
import os
import re

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-eu")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

BASE_URL = "https://www.carrefour.es"
SUPERMERCADO_URL = f"{BASE_URL}/supermercado"
DEFAULT_GROUP_URLS = [
    f"{BASE_URL}/supermercado/frescos/cat20002/c",
    f"{BASE_URL}/supermercado/la-despensa/cat20001/c",
    f"{BASE_URL}/supermercado/bebidas/cat20003/c",
]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"

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

BLOCKED_HTML_MARKERS = [
    "access denied",
    "request blocked",
    "checking your browser",
    "captcha",
]

PLAYWRIGHT_ENGINES = [e.strip().lower() for e in os.getenv("PLAYWRIGHT_ENGINES", "chromium").split(",") if e.strip()]
PLAYWRIGHT_DISABLE = os.getenv("PLAYWRIGHT_DISABLE", "false").lower() in {"1", "true", "yes"}
PLAYWRIGHT_MAX_RETRIES = max(1, int(os.getenv("PLAYWRIGHT_MAX_RETRIES", "1")))
PLAYWRIGHT_NAV_TIMEOUT_S = max(5, int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_S", "18")))
PLAYWRIGHT_SELECTOR_TIMEOUT_MS = max(0, int(os.getenv("PLAYWRIGHT_SELECTOR_TIMEOUT_MS", "3000")))
PLAYWRIGHT_CRASH_MARKERS = (
    "target page, context or browser has been closed",
    "browser has been closed",
    "process did exit",
    "sigsegv",
)

KEEP_LOCAL_FILES = os.getenv("KEEP_LOCAL_FILES", "false").lower()
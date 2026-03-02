"""
Configuration settings and environment variables for Dia scraper.
"""

import os

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-dev-eu")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

BASE_URL = "https://www.dia.es"
SUPERMERCADO_URL = f"{BASE_URL}/compra-online"

# Seed URLs used by targets_weekly. Can be overridden with DIA_GROUP_URLS.
DEFAULT_GROUP_URLS = [
    BASE_URL,
]

GROUP_LABELS = {
    "compra-online": "Compra Online",
    "frescos": "Frescos",
    "despensa": "Despensa",
    "bebidas": "Bebidas",
    "congelados": "Congelados",
    "limpieza": "Limpieza",
    "drogueria": "Drogueria",
    "hogar": "Hogar",
    "bebe": "Bebe",
    "mascotas": "Mascotas",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

OFFER_KEYWORDS = [
    "promocion",
    "promo",
    "oferta",
    "descuento",
    "2a unidad",
    "2 unidad",
    "rebaja",
    "pack ahorro",
]

BLOCKED_HTML_MARKERS = [
    "access denied",
    "request blocked",
    "checking your browser",
    "captcha",
]

PLAYWRIGHT_ENGINES = [
    e.strip().lower()
    for e in os.getenv("PLAYWRIGHT_ENGINES", "firefox,chromium").split(",")
    if e.strip()
]
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

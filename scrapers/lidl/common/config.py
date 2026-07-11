"""Configuration settings and environment variables for Lidl Spain scraper."""

import os

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-dev")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

BASE_URL = "https://www.lidl.es"
LEAFLET_API_BASE_URL = "https://endpoints.leaflets.schwarz/v4"

LEAFLET_INDEX_URL = os.getenv(
    "LIDL_LEAFLET_INDEX_URL",
    f"{BASE_URL}/c/descubre-nuevas-ofertas-cada-semana-folletos-lidl/s10087402",
)

DEFAULT_ONLINE_CATEGORY_URLS = [
    f"{BASE_URL}/c/cocina-y-cuidado-del-hogar/s10067764",
    f"{BASE_URL}/h/monsieur-cuisine/h10067521",
]

ONLINE_CATEGORY_URLS = [
    u.strip()
    for u in os.getenv("LIDL_ONLINE_CATEGORY_URLS", "").split(",")
    if u.strip()
] or DEFAULT_ONLINE_CATEGORY_URLS

ONLINE_API_VERSION = os.getenv("LIDL_ONLINE_API_VERSION", "v2.0.0")
ONLINE_ASSORTMENT = os.getenv("LIDL_ONLINE_ASSORTMENT", "ES")
ONLINE_LOCALE = os.getenv("LIDL_ONLINE_LOCALE", "es_ES")
ONLINE_FETCH_SIZE = max(1, int(os.getenv("LIDL_ONLINE_FETCH_SIZE", "48")))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_S = max(5, int(os.getenv("LIDL_TIMEOUT_S", "30")))
REQUEST_SLEEP_S = max(0.0, float(os.getenv("LIDL_SLEEP_S", "0.5")))
REQUEST_MAX_RETRIES = max(1, int(os.getenv("LIDL_MAX_RETRIES", "3")))

BLOCKED_HTML_MARKERS = [
    "access denied",
    "request blocked",
    "checking your browser",
    "captcha",
]

KEEP_LOCAL_FILES = os.getenv("KEEP_LOCAL_FILES", "false").lower()


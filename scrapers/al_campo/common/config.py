"""
Configuration settings and environment variables for Al Campo scraper.
"""
import os

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-dev")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

BASE_URL = "https://www.compraonline.alcampo.es"

DEFAULT_GROUP_URLS = [
    f"{BASE_URL}/categories/frescos/OC2112",
    f"{BASE_URL}/categories/leche-huevos-lácteos-yogures-y-bebidas-vegetales/OC16",
    f"{BASE_URL}/categories/bebidas/OCC11",
    f"{BASE_URL}/categories/congelados/OC200220183",
    f"{BASE_URL}/categories/comida-preparada/OC20022018",
    f"{BASE_URL}/categories/desayuno-y-merienda/OC10",
]

GROUP_LABELS = {
    "frescos": "Frescos",
    "leche-huevos-lacteos-yogures-y-bebidas-vegetales": "Leche, Huevos y Lácteos",
    "leche-huevos-lácteos-yogures-y-bebidas-vegetales": "Leche, Huevos y Lácteos",
    "bebidas": "Bebidas",
    "congelados": "Congelados",
    "comida-preparada": "Comida Preparada",
    "desayuno-y-merienda": "Desayuno y Merienda",
}

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"

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

PLAYWRIGHT_ENGINES = [e.strip().lower() for e in os.getenv("PLAYWRIGHT_ENGINES", "firefox,chromium").split(",") if e.strip()]
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

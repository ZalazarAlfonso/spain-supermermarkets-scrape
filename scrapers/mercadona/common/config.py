"""
Configuration settings and environment variables for Mercadona scraper.
"""

import os

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-dev")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

BASE_URL = "https://tienda.mercadona.es"
CATEGORIES_API_URL = f"{BASE_URL}/api/categories/"
CATEGORY_API_URL_TEMPLATE = f"{BASE_URL}/api/categories/{{category_id}}/"
PRODUCT_API_URL_TEMPLATE = f"{BASE_URL}/api/products/{{product_id}}/"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT_S = max(5, int(os.getenv("MERCADONA_TIMEOUT_S", "30")))
REQUEST_SLEEP_S = max(0.0, float(os.getenv("MERCADONA_SLEEP_S", "0.2")))
REQUEST_MAX_RETRIES = max(1, int(os.getenv("MERCADONA_MAX_RETRIES", "3")))

KNOWN_BRANDS = [
    "Hacendado",
    "Bosque Verde",
    "Deliplus",
    "Compy",
    "Royal",
    "Casa Tarradellas",
    "Danone",
    "Coca-Cola",
    "Coca Cola",
    "Pepsi",
    "Alpro",
    "Fanta",
    "Sprite",
    "Aquarius",
    "Nestea",
]

KEEP_LOCAL_FILES = os.getenv("KEEP_LOCAL_FILES", "false").lower()

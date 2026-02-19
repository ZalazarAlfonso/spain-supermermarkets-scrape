# jobs/carrefour/common/__init__.py

from .config import (
    GCS_BUCKET,
    GCS_PREFIX,
    GCS_PROJECT,
    BASE_URL,
    SUPERMERCADO_URL,
    DEFAULT_GROUP_URLS,
    USER_AGENT,
    PRICE_RE,
    OFFER_KEYWORDS,
    BLOCKED_HTML_MARKERS,
    PLAYWRIGHT_ENGINES,
    PLAYWRIGHT_DISABLE,
    PLAYWRIGHT_MAX_RETRIES,
    PLAYWRIGHT_NAV_TIMEOUT_S,
    PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
    PLAYWRIGHT_CRASH_MARKERS,
    KEEP_LOCAL_FILES,
)

from .gcs import (
    upload_file, 
    read_file_text, 
    validate_gcs_upload_config
)

from .http import (
    fetch
)

from .models import (
    ProductRow,
    CategoryTarget
)

from .parsing import (
    normalize_url,
    is_category_url,
    is_product_url,
    soup_from_html,
    extract_links,
    extract_pagination_links,
    extract_product_links,
    extract_product_links_from_html,
    extract_offer_flag,
    parse_product_card,
    group_slug_from_url,
)

__all__ = [
    # Config
    "GCS_BUCKET",
    "GCS_PREFIX",
    "GCS_PROJECT",
    "BASE_URL",
    "SUPERMERCADO_URL",
    "DEFAULT_GROUP_URLS",
    "USER_AGENT",
    "PRICE_RE",
    "OFFER_KEYWORDS",
    "BLOCKED_HTML_MARKERS",
    "PLAYWRIGHT_ENGINES",
    "PLAYWRIGHT_DISABLE",
    "PLAYWRIGHT_MAX_RETRIES",
    "PLAYWRIGHT_NAV_TIMEOUT_S",
    "PLAYWRIGHT_SELECTOR_TIMEOUT_MS",
    "PLAYWRIGHT_CRASH_MARKERS",
    "KEEP_LOCAL_FILES",
    # GCS
    "upload_file",
    "read_file_text",
    "validate_gcs_upload_config",
    # HTTP
    "fetch",
    # Models
    "ProductRow",
    "CategoryTarget",
    # Parsing
    "normalize_url",
    "is_category_url",
    "is_product_url",
    "soup_from_html",
    "extract_links",
    "extract_pagination_links",
    "extract_product_links",
    "extract_product_links_from_html",
    "extract_offer_flag",
    "parse_product_card",
    "group_slug_from_url",
]
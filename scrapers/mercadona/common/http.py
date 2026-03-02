"""
HTTP utilities for Mercadona API requests.
"""

from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import REQUEST_MAX_RETRIES, REQUEST_TIMEOUT_S, USER_AGENT


def build_session() -> requests.Session:
    """Create a requests session with retries and Mercadona-friendly headers."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Referer": "https://tienda.mercadona.es/",
        }
    )
    retries = Retry(
        total=REQUEST_MAX_RETRIES,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def fetch_json(session: requests.Session, url: str, timeout: int = REQUEST_TIMEOUT_S) -> Dict[str, Any]:
    """Fetch and parse a JSON response."""
    resp = session.get(url, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object from {url}, got {type(data).__name__}")
    return data

# jobs/carrefour/common/http.py
import requests
from .config import (
    USER_AGENT,
    BLOCKED_HTML_MARKERS,
    PLAYWRIGHT_ENGINES,
    PLAYWRIGHT_DISABLE,
    PLAYWRIGHT_MAX_RETRIES,
    PLAYWRIGHT_NAV_TIMEOUT_S,
    PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
    PLAYWRIGHT_CRASH_MARKERS,
)
import time
from typing import Dict, Iterable, List, Optional, Set, Tuple
try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None

def should_fallback_to_playwright(status_code: int, html: str) -> bool:
    if status_code == 403:
        return True
    low = html.lower()
    return any(marker in low for marker in BLOCKED_HTML_MARKERS)


def fetch_with_playwright(url: str, timeout: int = 40) -> str:
    """Fetch rendered HTML with Playwright."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required to bypass 403/JS rendering on carrefour.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright fallback is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None

    with sync_playwright() as p:
        for attempt in range(1, PLAYWRIGHT_MAX_RETRIES + 1):
            for engine_name in PLAYWRIGHT_ENGINES:
                browser = None
                context = None
                if not hasattr(p, engine_name):
                    print(f"[WARN] unsupported Playwright engine '{engine_name}'")
                    continue
                try:
                    browser_type = getattr(p, engine_name)
                    launch_args = []
                    if engine_name == "chromium":
                        launch_args = [
                            "--disable-gpu",
                            "--disable-software-rasterizer",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                        ]
                    browser = browser_type.launch(
                        headless=True,
                        args=launch_args,
                    )
                    context = browser.new_context(
                        locale="es-ES",
                        user_agent=USER_AGENT,
                    )
                    page = context.new_page()
                    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
                    if PLAYWRIGHT_SELECTOR_TIMEOUT_MS > 0:
                        try:
                            page.wait_for_selector(
                                "div.nav-second-level-categories, li.product-card-list__item, div.product-card",
                                timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
                            )
                        except Exception:
                            pass
                    return page.content()
                except Exception as exc:
                    last_exc = exc
                    msg = str(exc).lower()
                    print(
                        f"[WARN] Playwright {engine_name} failed "
                        f"(attempt={attempt}/{PLAYWRIGHT_MAX_RETRIES}) url={url}: {exc}"
                    )
                    if any(marker in msg for marker in PLAYWRIGHT_CRASH_MARKERS):
                        time.sleep(0.75 * attempt)
                finally:
                    if context is not None:
                        try:
                            context.close()
                        except Exception:
                            pass
                    if browser is not None:
                        try:
                            browser.close()
                        except Exception:
                            pass

    raise RuntimeError(f"Playwright fetch failed for {url}") from last_exc

def fetch(session: requests.Session, url: str, timeout: int = 30) -> str:
    """Fetch HTML; fall back to Playwright on anti-bot responses."""
    resp = session.get(url, timeout=timeout, allow_redirects=True)
    html = resp.text

    if should_fallback_to_playwright(resp.status_code, html):
        print(
            f"[FETCH] fallback_to_playwright status={resp.status_code} url={url}"
        )
        return fetch_with_playwright(url, timeout=PLAYWRIGHT_NAV_TIMEOUT_S)

    resp.raise_for_status()

    return html

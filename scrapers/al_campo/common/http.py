# jobs/al_campo/common/http.py
import requests
from bs4 import BeautifulSoup
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
from typing import Dict, List, Optional

from . import parsing

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None


def should_fallback_to_playwright(status_code: int, html: str) -> bool:
    if status_code == 403:
        return True
    low = html.lower()
    return any(marker in low for marker in BLOCKED_HTML_MARKERS)


def _launch_browser_and_fetch(p, engine_name: str, url: str, nav_timeout_ms: int, extra_wait_fn=None) -> str:
    """Helper that launches a single browser engine, fetches url, and returns page content."""
    launch_args = []
    if engine_name == "chromium":
        launch_args = [
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ]
    browser = getattr(p, engine_name).launch(headless=True, args=launch_args)
    context = browser.new_context(locale="es-ES", user_agent=USER_AGENT)
    try:
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
        if extra_wait_fn:
            extra_wait_fn(page)
        return page.content()
    finally:
        context.close()
        browser.close()


def fetch_with_playwright(url: str, timeout: int = 40) -> str:
    """Fetch rendered HTML with Playwright (simple, no scrolling)."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required to bypass 403/JS rendering on compraonline.alcampo.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright fallback is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000

    def _wait_for_selector(page):
        if PLAYWRIGHT_SELECTOR_TIMEOUT_MS > 0:
            try:
                page.wait_for_selector(
                    "a.product-card-container, nav, [class*='category']",
                    timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
                )
            except Exception:
                pass

    with sync_playwright() as p:
        for attempt in range(1, PLAYWRIGHT_MAX_RETRIES + 1):
            for engine_name in PLAYWRIGHT_ENGINES:
                if not hasattr(p, engine_name):
                    print(f"[WARN] unsupported Playwright engine '{engine_name}'")
                    continue
                try:
                    return _launch_browser_and_fetch(p, engine_name, url, nav_timeout_ms, _wait_for_selector)
                except Exception as exc:
                    last_exc = exc
                    print(
                        f"[WARN] Playwright {engine_name} failed "
                        f"(attempt={attempt}/{PLAYWRIGHT_MAX_RETRIES}) url={url}: {exc}"
                    )
                    if any(marker in str(exc).lower() for marker in PLAYWRIGHT_CRASH_MARKERS):
                        time.sleep(0.75 * attempt)

    raise RuntimeError(f"Playwright fetch failed for {url}") from last_exc


def fetch(session: requests.Session, url: str, timeout: int = 30) -> str:
    """Fetch HTML; fall back to Playwright on anti-bot responses."""
    resp = session.get(url, timeout=timeout, allow_redirects=True)
    html = resp.text

    if should_fallback_to_playwright(resp.status_code, html):
        print(f"[FETCH] fallback_to_playwright status={resp.status_code} url={url}")
        return fetch_with_playwright(url, timeout=PLAYWRIGHT_NAV_TIMEOUT_S)

    resp.raise_for_status()
    return html


def fetch_scrolled(url: str, timeout: int = 60) -> str:
    """Fetch full rendered HTML after scrolling to trigger all lazy-loaded products.

    Al Campo uses infinite scroll — products load as the user scrolls down.
    This function:
      1. Navigates to the URL and waits for the first product card to appear.
      2. Repeatedly scrolls to the bottom and waits for new products to load.
      3. Stops when the product count stabilises (no new cards after a scroll).
    Always uses Playwright; no requests fallback (page requires JS).
    """
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for fetch_scrolled on compraonline.alcampo.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000

    with sync_playwright() as p:
        for attempt in range(1, PLAYWRIGHT_MAX_RETRIES + 1):
            for engine_name in PLAYWRIGHT_ENGINES:
                if not hasattr(p, engine_name):
                    print(f"[WARN] unsupported Playwright engine '{engine_name}'")
                    continue
                browser = None
                context = None
                try:
                    launch_args = []
                    if engine_name == "chromium":
                        launch_args = [
                            "--disable-gpu",
                            "--disable-software-rasterizer",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                        ]
                    browser = getattr(p, engine_name).launch(headless=True, args=launch_args)
                    context = browser.new_context(locale="es-ES", user_agent=USER_AGENT)
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)

                    # Wait for the first product card to appear
                    try:
                        selector_timeout = max(12_000, PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
                        page.wait_for_selector(
                            "a.product-card-container, div.product-card-container",
                            timeout=selector_timeout,
                        )
                    except Exception:
                        # Page may have no products or different structure; proceed anyway
                        pass

                    # Scroll loop: keep scrolling until product count stabilises
                    prev_count = 0
                    stagnant_rounds = 0
                    zero_rounds = 0
                    max_iterations = 80
                    max_stagnant_rounds = 3
                    for _ in range(max_iterations):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        try:
                            page.wait_for_function(
                                "prev => document.querySelectorAll('a.product-card-container, div.product-card-container').length > prev",
                                arg=prev_count,
                                timeout=2500,
                            )
                        except Exception:
                            pass
                        time.sleep(1.2)
                        current_count = page.evaluate(
                            "document.querySelectorAll('a.product-card-container, div.product-card-container').length"
                        )

                        if current_count == 0:
                            zero_rounds += 1
                            # Give the app a chance to hydrate before declaring failure.
                            if zero_rounds >= 8:
                                break
                            continue

                        zero_rounds = 0
                        if current_count <= prev_count:
                            stagnant_rounds += 1
                        else:
                            stagnant_rounds = 0
                        if stagnant_rounds >= max_stagnant_rounds:
                            break
                        prev_count = current_count

                    return page.content()

                except Exception as exc:
                    last_exc = exc
                    print(
                        f"[WARN] fetch_scrolled {engine_name} failed "
                        f"(attempt={attempt}/{PLAYWRIGHT_MAX_RETRIES}) url={url}: {exc}"
                    )
                    if any(marker in str(exc).lower() for marker in PLAYWRIGHT_CRASH_MARKERS):
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

    raise RuntimeError(f"fetch_scrolled failed for {url}") from last_exc


def fetch_scrolled_products(url: str, timeout: int = 60) -> List[Dict[str, str]]:
    """Collect products while scrolling to handle virtualized Alcampo category lists."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for fetch_scrolled_products on compraonline.alcampo.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000

    with sync_playwright() as p:
        for attempt in range(1, PLAYWRIGHT_MAX_RETRIES + 1):
            for engine_name in PLAYWRIGHT_ENGINES:
                if not hasattr(p, engine_name):
                    print(f"[WARN] unsupported Playwright engine '{engine_name}'")
                    continue
                browser = None
                context = None
                try:
                    launch_args = []
                    if engine_name == "chromium":
                        launch_args = [
                            "--disable-gpu",
                            "--disable-software-rasterizer",
                            "--disable-dev-shm-usage",
                            "--no-sandbox",
                        ]
                    browser = getattr(p, engine_name).launch(headless=True, args=launch_args)
                    context = browser.new_context(locale="es-ES", user_agent=USER_AGENT)
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)

                    try:
                        selector_timeout = max(12_000, PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
                        page.wait_for_selector(
                            "a.product-card-container, div.product-card-container",
                            timeout=selector_timeout,
                        )
                    except Exception:
                        pass

                    by_url: Dict[str, Dict[str, str]] = {}
                    stagnant_rounds = 0
                    max_iterations = 140

                    for _ in range(max_iterations):
                        html = page.content()
                        soup: BeautifulSoup = parsing.soup_from_html(html)
                        cards = parsing.extract_product_cards(soup)

                        before = len(by_url)
                        for card in cards:
                            product_url = parsing.extract_product_url(card)
                            if not product_url:
                                continue
                            name, brand, price, price_per_unit, offer = parsing.parse_product_card(card)
                            by_url[product_url] = {
                                "product": name,
                                "brand": brand,
                                "price": price,
                                "price_per_unit": price_per_unit,
                                "offer": "true" if offer else "false",
                                "product_url": product_url,
                            }

                        added = len(by_url) - before
                        metrics = page.evaluate(
                            "() => ({y: window.scrollY, h: Math.max(document.body.scrollHeight, document.documentElement.scrollHeight), vh: window.innerHeight})"
                        )
                        at_bottom = (metrics["y"] + metrics["vh"]) >= (metrics["h"] - 8)
                        if added == 0 and at_bottom:
                            stagnant_rounds += 1
                        else:
                            stagnant_rounds = 0
                        if stagnant_rounds >= 6:
                            break

                        page.evaluate("window.scrollBy(0, 480)")
                        time.sleep(0.9)

                    if by_url:
                        return list(by_url.values())

                    return []

                except Exception as exc:
                    last_exc = exc
                    print(
                        f"[WARN] fetch_scrolled_products {engine_name} failed "
                        f"(attempt={attempt}/{PLAYWRIGHT_MAX_RETRIES}) url={url}: {exc}"
                    )
                    if any(marker in str(exc).lower() for marker in PLAYWRIGHT_CRASH_MARKERS):
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

    raise RuntimeError(f"fetch_scrolled_products failed for {url}") from last_exc

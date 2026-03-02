import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from . import parsing
from .config import (
    BLOCKED_HTML_MARKERS,
    PLAYWRIGHT_CRASH_MARKERS,
    PLAYWRIGHT_DISABLE,
    PLAYWRIGHT_ENGINES,
    PLAYWRIGHT_MAX_RETRIES,
    PLAYWRIGHT_NAV_TIMEOUT_S,
    PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
    USER_AGENT,
)

try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None


PRODUCT_SELECTOR = ", ".join(
    [
        "a[href*='/p/']",
        "a[href*='/producto/']",
        "a[href*='/product/']",
        "a[href*='/products/']",
        "article[class*='product']",
        "div[class*='product-card']",
        "li[class*='product']",
    ]
)


def should_fallback_to_playwright(status_code: int, html: str) -> bool:
    if status_code == 403:
        return True
    low = html.lower()
    return any(marker in low for marker in BLOCKED_HTML_MARKERS)


def _dismiss_consent_banner(page) -> bool:
    """Try to dismiss common cookie/consent banners that block interactions."""
    for consent_selector in (
        "button#onetrust-accept-btn-handler",
        "[data-test-id='accept-cookies']",
        "button[aria-label*='acept']",
        "button[aria-label*='accept']",
    ):
        try:
            page.click(consent_selector, timeout=1200)
            return True
        except Exception:
            continue
    return False


def _launch_browser_and_fetch(p, engine_name: str, url: str, nav_timeout_ms: int, extra_wait_fn=None) -> str:
    launch_args = []
    if engine_name == "chromium":
        launch_args = [
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ]

    browser = getattr(p, engine_name).launch(headless=False, args=launch_args)
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
    """Fetch rendered HTML with Playwright (no scrolling)."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required to bypass 403/JS rendering on dia.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright fallback is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000

    def _wait_for_selector(page):
        if PLAYWRIGHT_SELECTOR_TIMEOUT_MS <= 0:
            return
        try:
            page.wait_for_selector(PRODUCT_SELECTOR, timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
        except Exception:
            # Category pages may not have products above the fold.
            try:
                page.wait_for_selector("a[href*='compra-online'], nav", timeout=PLAYWRIGHT_SELECTOR_TIMEOUT_MS)
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


def fetch_with_playwright_interaction(
    url: str,
    timeout: int = 40,
    click_selector: Optional[str] = None,
    wait_selector: Optional[str] = None,
    mobile_viewport: bool = False,
    post_click_scroll_steps: int = 0,
) -> str:
    """Fetch rendered HTML with optional click + selector wait actions.

    Useful for pages where target elements are only visible after UI interaction
    (e.g. opening Dia categories from the mobile categories button).
    """
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for interactive rendering on dia.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright fallback is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000
    selector_timeout = max(3000, PLAYWRIGHT_SELECTOR_TIMEOUT_MS)

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
                    context = browser.new_context(
                        locale="es-ES",
                        user_agent=USER_AGENT,
                        viewport={"width": 390, "height": 844} if mobile_viewport else None,
                    )
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)

                    _dismiss_consent_banner(page)

                    if click_selector:
                        try:
                            page.wait_for_selector(click_selector, timeout=selector_timeout)
                            page.click(click_selector)
                        except Exception:
                            # If click is not possible, continue: content may already be visible.
                            pass

                    if wait_selector:
                        try:
                            page.wait_for_selector(wait_selector, timeout=max(8_000, selector_timeout))
                        except Exception:
                            pass

                    # Scroll page and any scrollable panels to trigger lazy-loaded menu items.
                    for _ in range(max(0, post_click_scroll_steps)):
                        try:
                            page.evaluate("window.scrollBy(0, 420)")
                        except Exception:
                            pass
                        try:
                            page.evaluate(
                                """
                                () => {
                                  const nodes = Array.from(document.querySelectorAll('*'));
                                  for (const el of nodes) {
                                    const style = window.getComputedStyle(el);
                                    const canScroll =
                                      (style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                                      el.scrollHeight > el.clientHeight;
                                    if (canScroll) el.scrollTop = el.scrollHeight;
                                  }
                                }
                                """
                            )
                        except Exception:
                            pass
                        time.sleep(0.2)

                    return page.content()

                except Exception as exc:
                    last_exc = exc
                    print(
                        f"[WARN] Playwright interaction {engine_name} failed "
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

    raise RuntimeError(f"Playwright interaction fetch failed for {url}") from last_exc


def collect_interactive_links(
    url: str,
    click_selectors: List[str],
    link_selector: str,
    title_selector: Optional[str] = None,
    wait_selector: Optional[str] = None,
    timeout: int = 40,
    mobile_viewport: bool = False,
    max_scroll_steps: int = 50,
    click_chain: Optional[List[Dict[str, object]]] = None,
) -> List[Dict[str, str]]:
    """Collect links while scrolling a possibly-virtualized UI list.

    Returns a list like: [{"title": "...", "url": "https://..."}, ...]
    """
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for interactive link collection on dia.es. "
            "Install with: pip install playwright && playwright install"
        )
    if PLAYWRIGHT_DISABLE:
        raise RuntimeError("Playwright fallback is disabled by PLAYWRIGHT_DISABLE=true")

    last_exc: Optional[Exception] = None
    nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000
    selector_timeout = max(3000, PLAYWRIGHT_SELECTOR_TIMEOUT_MS)

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
                    context = browser.new_context(
                        locale="es-ES",
                        user_agent=USER_AGENT,
                        viewport={"width": 390, "height": 844} if mobile_viewport else None,
                    )
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)

                    _dismiss_consent_banner(page)

                    if click_chain:
                        for idx, step in enumerate(click_chain, start=1):
                            selector = str(step.get("selector") or "").strip()
                            if not selector:
                                continue
                            should_click = bool(step.get("click", True))
                            required = bool(step.get("required", False))
                            wait_ms = int(step.get("wait_ms", selector_timeout))
                            label = str(step.get("label") or f"step-{idx}")
                            try:
                                page.wait_for_selector(selector, timeout=max(500, wait_ms))
                                if should_click:
                                    page.click(selector)
                                    time.sleep(0.2)
                            except Exception as exc:
                                if required:
                                    raise RuntimeError(
                                        f"click_chain required step failed label={label} selector={selector}"
                                    ) from exc

                    for selector in click_selectors:
                        if not selector:
                            continue
                        try:
                            page.wait_for_selector(selector, timeout=3000)
                            page.click(selector)
                            time.sleep(0.2)
                        except Exception:
                            continue

                    if wait_selector:
                        try:
                            page.wait_for_selector(wait_selector, timeout=max(8_000, selector_timeout))
                        except Exception:
                            pass

                    by_url: Dict[str, Dict[str, str]] = {}
                    stagnant_rounds = 0
                    max_rounds_stagnant = 6

                    for _ in range(max(1, max_scroll_steps)):
                        payload = page.evaluate(
                            """
                            ({linkSelector, titleSelector}) => {
                              const nodes = Array.from(document.querySelectorAll(linkSelector));
                              const items = [];
                              for (const node of nodes) {
                                const anchor = node.tagName === 'A' ? node : node.closest('a') || node.querySelector('a[href]');
                                if (!anchor) continue;
                                const href = anchor.getAttribute('href') || '';
                                if (!href) continue;
                                let title = '';
                                if (titleSelector) {
                                  const tEl = node.querySelector(titleSelector) || anchor.querySelector(titleSelector);
                                  if (tEl) title = (tEl.textContent || '').trim();
                                }
                                if (!title) title = (anchor.textContent || '').trim();
                                items.push({href, title});
                              }

                              let scroller = null;
                              const pivot = nodes.length ? (nodes[0].parentElement || nodes[0]) : null;
                              let el = pivot;
                              while (el) {
                                const style = window.getComputedStyle(el);
                                const canScroll =
                                  (style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                                  el.scrollHeight > el.clientHeight + 2;
                                if (canScroll) {
                                  scroller = el;
                                  break;
                                }
                                el = el.parentElement;
                              }

                              let atBottom = false;
                              if (scroller) {
                                const step = Math.max(360, Math.floor(scroller.clientHeight * 0.8));
                                scroller.scrollTop = Math.min(scroller.scrollTop + step, scroller.scrollHeight);
                                atBottom = (scroller.scrollTop + scroller.clientHeight) >= (scroller.scrollHeight - 2);
                              } else {
                                window.scrollBy(0, 420);
                                const h = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                                atBottom = (window.scrollY + window.innerHeight) >= (h - 4);
                              }

                              return {items, atBottom};
                            }
                            """,
                            {"linkSelector": link_selector, "titleSelector": title_selector or ""},
                        )

                        before = len(by_url)
                        for item in payload.get("items", []):
                            href = str(item.get("href") or "").strip()
                            abs_url = parsing.normalize_url(href) or ""
                            if not abs_url:
                                continue
                            by_url[abs_url] = {
                                "title": str(item.get("title") or "").strip(),
                                "url": abs_url,
                            }

                        added = len(by_url) - before
                        at_bottom = bool(payload.get("atBottom"))
                        if added == 0 and at_bottom:
                            stagnant_rounds += 1
                        else:
                            stagnant_rounds = 0
                        if stagnant_rounds >= max_rounds_stagnant:
                            break

                        time.sleep(0.2)

                    return list(by_url.values())

                except Exception as exc:
                    last_exc = exc
                    print(
                        f"[WARN] Playwright collect links {engine_name} failed "
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

    raise RuntimeError(f"Playwright link collection failed for {url}") from last_exc


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
    """Fetch rendered HTML after scrolling to trigger lazy-loaded products."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for fetch_scrolled on dia.es. "
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
                        page.wait_for_selector(PRODUCT_SELECTOR, timeout=selector_timeout)
                    except Exception:
                        pass

                    prev_count = 0
                    stagnant_rounds = 0
                    zero_rounds = 0
                    max_iterations = 80
                    max_stagnant_rounds = 3

                    for _ in range(max_iterations):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        try:
                            page.wait_for_function(
                                "([selector, prev]) => document.querySelectorAll(selector).length > prev",
                                arg=[PRODUCT_SELECTOR, prev_count],
                                timeout=2500,
                            )
                        except Exception:
                            pass

                        time.sleep(1.2)
                        current_count = page.evaluate(
                            "selector => document.querySelectorAll(selector).length",
                            PRODUCT_SELECTOR,
                        )

                        if current_count == 0:
                            zero_rounds += 1
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
    """Collect products while scrolling to handle virtualized category lists."""
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for fetch_scrolled_products on dia.es. "
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
                        page.wait_for_selector(PRODUCT_SELECTOR, timeout=selector_timeout)
                    except Exception:
                        pass

                    by_url: Dict[str, Dict[str, str]] = {}
                    by_name: Dict[str, Dict[str, str]] = {}
                    stagnant_rounds = 0
                    max_iterations = 140

                    for _ in range(max_iterations):
                        html = page.content()
                        soup: BeautifulSoup = parsing.soup_from_html(html)
                        cards = parsing.extract_product_cards(soup)

                        before = len(by_url) + len(by_name)
                        for card in cards:
                            product_url = parsing.extract_product_url(card)
                            name, brand, price, price_per_unit, offer = parsing.parse_product_card(card)

                            if not product_url and not name:
                                continue

                            row = {
                                "product": name,
                                "brand": brand,
                                "price": price,
                                "price_per_unit": price_per_unit,
                                "offer": "true" if offer else "false",
                                "product_url": product_url,
                            }

                            if product_url:
                                by_url[product_url] = row
                            else:
                                by_name[name.lower()] = row

                        if not by_url:
                            for row in parsing.extract_products_from_json_ld(soup):
                                product_url = (row.get("product_url") or "").strip()
                                name = (row.get("product") or "").strip().lower()
                                if product_url:
                                    by_url[product_url] = row
                                elif name:
                                    by_name[name] = row

                        after = len(by_url) + len(by_name)
                        added = after - before

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

                    if by_url or by_name:
                        existing_names = {(r.get("product") or "").lower() for r in by_url.values()}
                        merged = list(by_url.values()) + [row for key, row in by_name.items() if key not in existing_names]
                        return merged

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

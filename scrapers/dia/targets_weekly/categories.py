"""
Manual debug-only script.

Production DIA category discovery lives in:
- jobs/dia/targets_weekly/main.py
- jobs/dia/common/http.py
"""

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import time
try:
    from playwright.sync_api import sync_playwright  # type: ignore
except Exception:  # pragma: no cover
    sync_playwright = None

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
PLAYWRIGHT_NAV_TIMEOUT_S = 3000
PLAYWRIGHT_SELECTOR_TIMEOUT_MS = 4000

session = requests.Session()
session.headers.update(
    {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }
)
retries = Retry(
    total=2,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
)
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

url = 'https://www.dia.es/'

resp = session.get(url, allow_redirects=True)
html = resp.text
soup = BeautifulSoup(html, "html.parser")
categories = soup.find("div", class_="nav-second-level-categories")
engine_name = 'firefox'
timeout = 3000
with sync_playwright() as p:

    browser = None
    context = None
    if not hasattr(p, engine_name):
        print(f"[WARN] unsupported Playwright engine '{engine_name}'")
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
            headless=False,
            args=launch_args,
        )
        context = browser.new_context(
            locale="es-ES",
            user_agent=USER_AGENT,
        )
        page = context.new_page()
        nav_timeout_ms = min(timeout, PLAYWRIGHT_NAV_TIMEOUT_S) * 1000
        page.goto(url, timeout=nav_timeout_ms)

        # Wait for cookie banner and click Accept
        page.wait_for_selector("#onetrust-accept-btn-handler", timeout=10000)
        page.click("#onetrust-accept-btn-handler")

        print("Cookies accepted")

        # Wait until button is visible
        page.wait_for_selector('button[data-test-id="desktop-category-button"]')

        # Click it
        page.click('button[data-test-id="desktop-category-button"]')

        # Wait for categories list
        page.wait_for_selector('ul[data-test-id="categories-list"]')
        ul = page.locator('ul[data-test-id="categories-list"]')

        # Why: ensure the menu is actually there and visible before extracting
        ul.wait_for(state="visible")

        rows = []
        lis = ul.locator(":scope > li")
        n = lis.count()

        for i in range(n):
            li = lis.nth(i)

            # locate the anchor inside each li (if present)
            a = li.locator("a").first

            name = a.inner_text().strip() if a.count() else li.inner_text().strip()
            href = a.get_attribute("href") if a.count() else None

            rows.append({"name": name, "href": href})

        print(type(rows))

        for row in rows:
            print(f"Category: {row['name']}")
            page.goto(url+row['href'])
            page.wait_for_selector('ul[data-test-id="sub-categories-list"]')
            subcat_ul = page.locator('ul[data-test-id="sub-categories-list"]')
            subcat_ul.wait_for(state="visible")
            subcats_lis = subcat_ul.locator(":scope > div")
            print(f"Subcategories: {subcats_lis.count()}")
            print(f"texts: {subcats_lis.all_inner_texts()}")
            time.sleep(5)

        browser.close()
    except Exception as exc:
        last_exc = exc
        msg = str(exc).lower()

# Lidl Spain (lidl.es) — Scraping Feasibility Research

**Date:** 2026-03-02
**Branch:** `claude/research-lidl-scraping-49bW2`

---

## Summary

Scraping `lidl.es` is **technically feasible but moderately difficult** due to Cloudflare protection.
Simple `requests`-based HTTP fetching is blocked with 403. A Playwright-based headless browser
approach (already used in this repo for Carrefour/Al Campo/Dia) is the right tool, but stealth
configuration will be important. The site is a JavaScript-rendered SPA.

---

## 1. Site Technology & Rendering

| Attribute | Finding |
|-----------|---------|
| Rendering | **JavaScript SPA** — the product grid is rendered client-side |
| CDN / WAF | **Cloudflare** (ASN AS13335) |
| Plain HTTP | **Blocked (403)** for all product/category pages |
| Playwright | Required; stealth mode recommended |

Direct `requests` calls to any category or product page return **HTTP 403**. The site uses
Cloudflare's bot detection stack (JS challenges, TLS fingerprinting, behavioral analysis).
Playwright with proper browser fingerprinting (existing `http.py` pattern) should be sufficient
for non-aggressive crawling.

---

## 2. robots.txt

`https://www.lidl.es/robots.txt` also returns 403 when fetched programmatically. The page is
accessible from a real browser. Based on the site structure (fully JavaScript-rendered), the
robots.txt likely disallows automated crawling of most paths, but this needs to be confirmed
via a real browser session.

**Action needed:** Inspect `robots.txt` manually via a browser before implementing.

---

## 3. URL Structure

Lidl uses a consistent URL pattern across all country domains:

| Page type | Pattern | Example |
|-----------|---------|---------|
| Category listing | `/c/{category-name}/{category-id}` | `https://www.lidl.es/c/frescos/a10063993` |
| Product detail | `/p/{product-name}/p{sku}` | `https://www.lidl.es/p/leche-entera/p12345678` |
| Supermarket section | `/c/supermercado/...` | Top-level grocery landing page |

Category IDs start with `a` or `h` depending on the hierarchy level. The sitemap at
`https://www.lidl.es/sitemap.xml` can be used to enumerate all category and product URLs
(needs Playwright to fetch).

**Known grocery-relevant categories on lidl.es:**
- Frescos (fresh produce, meat, fish, dairy)
- Despensa / Panadería
- Bebidas
- Congelados
- Limpieza del Hogar
- Cuidado Personal

---

## 4. Product Data Available

Based on third-party scrapers (Apify, ScrapeIt, ShoppingScraper) and the open-source
[jonathan9879/lidl-scraper](https://github.com/jonathan9879/lidl-scraper) (targets lidl.es
promotions), the following product fields are extractable:

| Field | Notes |
|-------|-------|
| `product` | Product name / title |
| `brand` | Brand name (Lidl own-brand or third party) |
| `price` | Current price (€) |
| `price_per_unit` | Price per kg / litre / unit |
| `offer` | Discount / promotional flag |
| `category` | Category name |
| `subcategory` | Subcategory name |
| `product_url` | Canonical product URL |
| `sku` | Internal SKU (from URL) |
| `image_url` | Product image URL |
| `ean` | EAN barcode (on product detail pages) |
| `description` | Product description |
| `availability` | In-stock flag |

The fields `product`, `brand`, `price`, `price_per_unit`, `offer`, `category`, `subcategory`,
and `product_url` directly match this repo's `ProductRow` model — **no schema changes needed**.

---

## 5. HTML / DOM Structure

The open-source `jonathan9879/lidl-scraper` (targets `lidl.es` promotions) uses:

```python
# Product grid items
products = soup.find_all(".productgrid__item")

# Promotional pages (weekly offers)
# URL pattern: /es/promo/... ending in week=1
```

For the main online supermarket (`/c/supermercado/...`), the DOM is rendered by React/Next.js.
Product cards likely use classes like:
- `product-grid-box` or `product-tile`
- `price-tag` or `pricebox__price`
- `product-name`

**Action needed:** Inspect the actual rendered DOM with Playwright to confirm the exact CSS
selectors before building the parser.

---

## 6. Anti-Scraping Measures

| Layer | Status | Mitigation |
|-------|--------|-----------|
| Cloudflare WAF | **Active** | Playwright with real browser fingerprint |
| JS Challenge | **Active** | Playwright handles JS execution natively |
| TLS Fingerprinting | **Active** | Playwright uses real Chromium TLS stack |
| Rate Limiting | Likely | Add `--sleep` between requests (2–5s recommended) |
| CAPTCHA | Triggered on abuse | Avoid aggressive crawling; respect `--max-pages` |
| IP blocking | On repeated 403s | Use `--sleep`, avoid parallel category scraping |

The existing `http.py` pattern (Playwright fallback on 403) is the correct approach. For Lidl,
Playwright should likely be the **first** fetch strategy (not the fallback), since the site
always requires JS rendering.

---

## 7. Implementation Plan

The implementation follows the exact same two-stage pipeline used for Carrefour/Al Campo/Dia:

### Stage 1 — `targets_weekly/main.py`
1. Fetch the Lidl Spain supermarket landing page (via Playwright)
2. Extract category navigation links (following the `/c/{category}/{id}` pattern)
3. For each category, check for subcategory navigation
4. Build a list of `CategoryTarget` objects and save as JSON

### Stage 2 — `scrape_daily/main.py`
1. Load category targets JSON
2. For each target URL, fetch the page with Playwright
3. Parse product cards using BeautifulSoup
4. Handle pagination (next-page links or load-more button)
5. Deduplicate by `product_url`
6. Write dated CSV (+ Parquet for GCS)

### New files needed (mirroring `scrapers/carrefour/`):

```
scrapers/lidl/
├── common/
│   ├── __init__.py
│   ├── config.py        # Lidl URLs, CSS selectors, env vars
│   ├── http.py          # Playwright-first fetch (copy + adjust from carrefour)
│   ├── parsing.py       # Lidl-specific CSS selectors
│   ├── models.py        # Same ProductRow / CategoryTarget TypedDicts
│   └── gcs.py           # Identical to carrefour (copy verbatim)
├── targets_weekly/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
└── scrape_daily/
    ├── main.py
    ├── Dockerfile
    └── requirements.txt
```

### Key config differences from Carrefour:

```python
# config.py
BASE_URL = "https://www.lidl.es"
SUPERMARKET_LANDING = f"{BASE_URL}/c/supermercado/a10007136"  # adjust ID after inspection

GROUP_URLS = [
    f"{BASE_URL}/c/frescos/a10063993",
    f"{BASE_URL}/c/despensa/a10063994",
    f"{BASE_URL}/c/bebidas/a10063995",
    f"{BASE_URL}/c/congelados/a10063996",
]

# Playwright should be the PRIMARY strategy, not the fallback
PLAYWRIGHT_AS_PRIMARY = True  # new flag
PLAYWRIGHT_ENGINES = os.getenv("PLAYWRIGHT_ENGINES", "chromium")
PLAYWRIGHT_NAV_TIMEOUT_S = int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_S", "25"))

# Wait for product grid to be rendered
PLAYWRIGHT_WAIT_SELECTOR = ".product-grid-box"  # confirm via DOM inspection
```

---

## 8. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| CSS selectors change | Medium | Parameterise selectors in `config.py`; monitor with alerts |
| Cloudflare upgrades detection | Medium | Keep Playwright up-to-date; add stealth args |
| Site structure change | Low–Medium | Two-stage pipeline isolates targets from scraping logic |
| robots.txt restrictions | Unknown | Inspect before implementing; add polite delays |
| Category IDs change | Low | Re-run `targets_weekly` to refresh |
| CAPTCHA triggered | Low (with delays) | Use `--sleep 3` minimum; avoid parallelism per category |

---

## 9. Conclusion

**Verdict: Go.** Implementation is feasible using the repo's existing Playwright-based HTTP layer
with minor adjustments:

1. Playwright should be the **primary** strategy (not fallback) for Lidl
2. Sleep between requests should default to **3–5 seconds** (more conservative than Carrefour)
3. CSS selectors need to be confirmed via a live browser session before coding the parser
4. Category IDs in the base URLs need to be verified (they may differ from the examples above)

The data model, pipeline architecture, and infrastructure (GCS, Docker, Cloud Build) are all
directly reusable from existing supermarket implementations.

---

## References

- [jonathan9879/lidl-scraper](https://github.com/jonathan9879/lidl-scraper) — Open-source lidl.es offer scraper (Python)
- [Apify Lidl Category Scraper](https://apify.com/getdataforme/lidl-category-scraper) — Third-party Lidl scraper actor
- [Apify Lidl Product Scraper](https://apify.com/easyapi/lidl-product-scraper/api/python) — Python API example
- [ScrapeIt Lidl Scraper](https://www.scrapeit.io/scraper/lidl) — Managed scraping service for Lidl
- [ShoppingScraper Lidl API](https://shoppingscraper.com/scrapers/lidl) — Commercial Lidl data API
- [Scrapfly — Bypass Cloudflare](https://scrapfly.io/blog/posts/how-to-bypass-cloudflare-anti-scraping) — Technical analysis of Cloudflare protection

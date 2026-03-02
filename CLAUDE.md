# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r jobs/carrefour/scrape_daily/requirements.txt

# Optional — only needed if Playwright fallback is required
playwright install
```

## Running the scrapers

Scripts must be run as modules from the repo root (so that `jobs/` is on the Python path):

**Step 1 — Discover category targets (run weekly):**
```bash
python -m jobs.carrefour.targets_weekly.main
# Options: --out, --sleep, --timeout, --max-categories, --upload-to-gcs
```

**Step 2 — Scrape daily products:**
```bash
python -m jobs.carrefour.scrape_daily.main
# Options: --out-dir, --sleep, --max-pages, --max-products, --max-categories, --upload-to-gcs
# --targets-source [local|gcs], --targets-local-path, --targets-gcs-bucket, --targets-gcs-object-name
```

**Quick smoke-test run (limits scope):**
```bash
python -m jobs.carrefour.scrape_daily.main --max-categories 2 --max-pages 1 --max-products 50
```

## Docker

Build context must be the repo root (the Dockerfile copies `jobs/` wholesale):
```bash
docker build -f jobs/carrefour/scrape_daily/Dockerfile -t carrefour-scraper .
docker run --rm -v "$PWD/jobs/carrefour/scrape_daily/files:/app/files" carrefour-scraper
```

Cloud Build: `cloudbuild.scrape_daily.yaml` (requires `$_IMAGE_URI` substitution variable).

## Architecture

The project follows a **two-stage pipeline per supermarket**:

```
targets_weekly/main.py   →  carrefour_categories.json  →  scrape_daily/main.py  →  CSV / GCS
```

### Job structure (`jobs/{supermarket}/`)

Each supermarket has two jobs and a shared `common/` package:

| Path | Purpose |
|------|---------|
| `targets_weekly/main.py` | Crawls group navigation pages to build a JSON list of `CategoryTarget` objects (group, category, subcategory, url) |
| `scrape_daily/main.py` | Reads the targets JSON, scrapes product cards from each URL, deduplicates by product URL, writes a dated CSV |
| `common/config.py` | All constants and env-var overrides (GCS bucket, Playwright settings, group URLs) |
| `common/http.py` | `fetch()` — tries `requests`, falls back to Playwright on 403 or blocked HTML markers |
| `common/parsing.py` | BeautifulSoup helpers: product card parser, pagination extractor, URL normalizer |
| `common/gcs.py` | GCS upload/download wrappers |
| `common/models.py` | `ProductRow` and `CategoryTarget` TypedDicts |

### Carrefour category logic

- Groups: **Frescos**, **La Despensa**, **Bebidas**
- Frescos / La Despensa: group page → categories → subcategory URLs (two-level nav)
- Bebidas: group page slides are directly subcategories (`category=Bebidas`)
- Slides whose name contains `oferta` (case-insensitive) are excluded
- If a category has no subcategory nav, it falls back to `subcategory=""`

### Env vars (via `.env` or shell)

| Variable | Default | Effect |
|----------|---------|--------|
| `GCS_BUCKET` | `azal-smarkets-raw-eu` | Upload target bucket |
| `GOOGLE_CLOUD_PROJECT` | `lab-spanish-smarkts-scraper` | GCP project |
| `CARREFOUR_GROUP_URLS` | three default URLs | Override crawl targets (comma-separated) |
| `KEEP_LOCAL_FILES` | `false` | Keep local CSV even after GCS upload |
| `PLAYWRIGHT_ENGINES` | `chromium` | Comma-separated engine order (e.g. `firefox,chromium`) |
| `PLAYWRIGHT_DISABLE` | `false` | Disable Playwright fallback entirely |
| `PLAYWRIGHT_MAX_RETRIES` | `1` | Retries per engine on crash |
| `PLAYWRIGHT_NAV_TIMEOUT_S` | `18` | Navigation timeout |

### Output

Daily CSV at `jobs/carrefour/scrape_daily/files/carrefour_supermercado_YYYY-MM-DD.csv`
Columns: `date`, `product`, `brand`, `price`, `price_per_unit`, `offer`, `category`, `subcategory`, `product_url`

GCS path pattern: `carrefour/{YYYY-MM-DD}/carrefour_supermercado_YYYY-MM-DD.csv`

### Al Campo

`jobs/al_campo/` mirrors the same layout (common/, scrape_daily/, targets_weekly/) but is not yet implemented.

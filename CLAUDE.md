# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Change control

Before editing files, explain the proposed change and ask for confirmation (see AGENTS.md).

## Pipeline

```
scrapers/{supermarket}/targets_weekly  ->  targets JSON (GCS)
scrapers/{supermarket}/scrape_daily    ->  {supermarket}/{YYYY-MM-DD}/*.parquet (GCS)
ingestion/bq_event_loader              ->  dwh_bronze_dev.bronze_{supermarket}_products_p
dbt/supermarket_dwh                    ->  dwh_silver_dev (staging, intermediate, silver), dwh_gold_dev (gold)
```

Supermarkets: `al_campo` (warehouse name `alcampo`), `carrefour`, `dia`, `mercadona`. `lidl` exists but is not loaded.

Bronze columns for every supermarket: `date`, `product`, `brand`, `price`, `price_per_unit`, `offer`, `category`, `subcategory`, `product_url`.

## Where things run (GCP project `lab-spanish-smarkts-scraper`)

Always pass `--project`: the gcloud default project is a different one.

| Piece | Runs on |
|-------|---------|
| Mercadona scraper | Cloud Run job `daily-supermarket-mercadona-scraper-dev` (europe-southwest1), scheduler `daily-supermarket-scraper-trigger-dev` 06:00 |
| Alcampo, Carrefour, Dia scrapers | Compute VMs `vm-instance-alcampo-daily`, `vm-instance-daily-carrefour`, `vm-instance-dia-daily` (europe-west9); a startup script pulls `...-scraper:latest` from Artifact Registry, runs it, shuts down |
| Bronze loader | Eventarc on GCS uploads -> `ingestion/bq_event_loader`; starts dbt once all four supermarkets have rows for the day |
| dbt | Cloud Run job `dbt-runner-dev`, scheduler `dbt-runner-daily-dev` 12:00 Europe/Madrid (europe-west1). Runbook: `docs/deploy_dbt_runner.md` |

Carrefour and Alcampo answer the VMs with 403 / blocked pages for most categories (since 2026-09-13 and 2026-07-30), so their daily scrapes are partial.

## Running a scraper locally

Run as a module from the repo root:

```bash
python -m scrapers.carrefour.targets_weekly.main
python -m scrapers.carrefour.scrape_daily.main --max-categories 2 --max-pages 1 --max-products 50
```

Docker build context is the repo root, e.g. `docker build -f scrapers/carrefour/scrape_daily/Dockerfile .` (Cloud Build: `cloudbuild.{supermarket}.scrape_daily.yaml`, needs `$_IMAGE_URI`).

## dbt

Use dbt-core, not the `dbt` on PATH (that is dbt Fusion and cannot parse this project; the yml uses the flat generic-test argument form on purpose). Versions are pinned in `dbt/supermarket_dwh/requirements.txt`.

Models in the `scrape_date` lineage are incremental (`insert_overwrite`, 3-day lookback), so a change to model logic or a seed only reaches history after `dbt build --full-refresh`.

Key models:

| Model | What it is |
|-------|-----------|
| `silver_product_standardized` | Every snapshot with standard taxonomy (`taxonomy_method`: rules, inherited_from_product, unmapped), brand, case size, `price_is_shelf_price` |
| `silver_supermarket_daily_completeness` | Rows per supermarket and day against usual volume; `is_complete` |
| `fact_products_today` | Current catalogue, each supermarket from its own latest complete scrape (`as_of_date`, `is_stale`) |
| `fact_product_price_snapshots` | Daily price history (incremental) |
| `fact_product_price_changes` | One row per observed price change |
| `supermarket_freshness` | Latest scrape vs latest complete scrape per supermarket |
| `dim_products`, `dim_taxonomy` | Product lifecycle and search text; taxonomy tree including 'Other' nodes |
| `qa_unmapped_products` | Source nodes no taxonomy rule places: add rules to `seeds/standard_rules_v1.csv` |

Mercadona prices scraped before the shelf-price fix are per kg/l: `price_is_shelf_price = false`. Never use them for basket totals.

# Spain Supermarket Scraper: Next Steps Plan

This plan splits the next project work into small, finishable tracks instead of one big roadmap. The recommended order is:

1. Finish the warehouse data product.
2. Automate bronze to silver to gold refreshes.
3. Build a small read API.
4. Build a simple website on top of that API.

The main goal is to make the scraped data reliable, queryable, and usable before adding too much frontend surface area.

## Current State

- Scrapers exist for Carrefour, Mercadona, Dia, and Alcampo.
- Daily scrapers can write Parquet files and upload them to GCS paths like `supermarket/YYYY-MM-DD/file.parquet`.
- `ingestion/bq_event_loader` can load new Parquet objects from GCS into BigQuery bronze tables.
- dbt already has staging, intermediate, silver, and gold folders.
- `fact_products_today` exists as an early gold table.
- Taxonomy/rule standardization work has started through `standard_rules_v1.csv`, `standard_taxonomy_v1.csv`, and dbt tests.

## Plan 1: Finish the Warehouse Fact Tables

### Target

Have a small, stable gold layer that answers the first real product questions:

- What products are available today?
- What is the latest price for each product?
- How has a product price changed over time?
- Which supermarket/category/subcategory does each product belong to?
- Which rows are usable for the API and website?

### Build These Gold Models

- `fact_products_today`
  - Purpose: latest available product row per `source_product_id`.
  - Status: started.
  - Finish by adding clear documentation, tests, and only API-ready columns.

- `fact_product_price_snapshots`
  - Purpose: one row per product, supermarket, scrape date, and observed price.
  - Source: `silver_product_snapshots` or `silver_product_standardized`.
  - Use for historical charts and price tracking.

- `fact_product_price_changes`
  - Purpose: compare each product's current price against its previous observed price.
  - Columns to include: `previous_price_value`, `price_change_value`, `price_change_pct`, `previous_scrape_date`.
  - Use for "price increased", "price dropped", and later alert-style features.

- `dim_products`
  - Purpose: stable product identity and metadata.
  - Columns to include: `source_product_id`, `supermarket`, `product_url`, `product_name`, `brand_raw`.

- `dim_taxonomy`
  - Purpose: standard category tree for website filters.
  - Columns to include: `group_std`, `category_std`, `subcategory_std`, sort order fields.

### Middle Goals

- Confirm the unique key for product facts:
  - likely `source_product_id` for current tables.
  - likely `source_product_id + scrape_date` for historical tables.
- Decide whether gold tables should include only standardized taxonomy values or also raw category fields.
- Add dbt tests for:
  - not null product ids.
  - not null prices in API-facing facts.
  - one current row per product.
  - accepted supermarket values.
  - taxonomy values present in the taxonomy seed, except `Other`.
- Add model docs in `gold.yml` for every gold model and important column.
- Run `dbt build` for staging, intermediate, silver, seeds, and gold.

### Done When

- `dbt build` passes.
- Gold tables can answer today's product list, product price history, and category filters.
- The API can be built without querying intermediate or silver models directly.

## Plan 2: Automate the ETL and dbt Refresh

### Target

Make the daily pipeline run without manual steps:

1. Weekly category discovery updates targets JSON.
2. Daily scrapers write Parquet to GCS.
3. GCS events load Parquet into bronze BigQuery tables.
4. dbt rebuilds intermediate, silver, and gold tables.
5. Failures are visible.

### Recommended Architecture

- Cloud Scheduler
  - Trigger weekly target discovery jobs.
  - Trigger daily scraper jobs.

- Cloud Run Jobs
  - One target discovery job per supermarket.
  - One scrape daily job per supermarket.
  - One dbt job for warehouse transformations.

- Eventarc or GCS notification
  - Keep the current `bq_event_loader` pattern for loading bronze when new Parquet lands.

- Workflow or scheduled dbt run
  - Preferred first version: run dbt on a daily schedule after scraper jobs should be finished.
  - Later version: trigger dbt only after all expected bronze tables have today's partition loaded.

### Middle Goals

- Standardize upload behavior across all scrapers.
  - Some scrapers upload by default via `UPLOAD_TO_GCS=true`.
  - Carrefour currently relies more directly on `--upload-to-gcs`.
  - Pick one convention and document it.
- Confirm bronze table schemas match the Parquet files from all four supermarkets.
- Add a small "pipeline readiness" query:
  - checks row counts by supermarket for today's date.
  - checks missing supermarket loads.
  - checks suspicious zero-row loads.
- Containerize a dbt runner.
  - It should run `dbt deps`, `dbt seed`, `dbt build`, or a narrower selected build.
  - It should use the BigQuery profile from environment/secrets.
- Add deploy docs or scripts for:
  - Eventarc loader.
  - Cloud Run scraper jobs.
  - Cloud Scheduler triggers.
  - dbt runner job.
- Add basic alerting:
  - failed Cloud Run job.
  - bronze load failure.
  - dbt build failure.
  - zero rows for a supermarket.

### Done When

- A new daily scrape lands in bronze without manual BigQuery work.
- dbt refreshes intermediate, silver, and gold automatically.
- There is a clear way to tell whether today's warehouse data is complete.

## Plan 3: Build the Read API

### Target

Create a small API that reads only from gold tables and powers a simple website.

### Recommended First API

Use FastAPI on Cloud Run with BigQuery as the backend.

Suggested endpoints:

- `GET /health`
  - Confirms the service is alive.

- `GET /products`
  - Query params: `supermarket`, `group`, `category`, `subcategory`, `search`, `limit`, `cursor`.
  - Source table: `fact_products_today`.

- `GET /products/{source_product_id}`
  - Returns one current product record.
  - Source table: `fact_products_today`.

- `GET /products/{source_product_id}/price-history`
  - Returns dated prices for charts.
  - Source table: `fact_product_price_snapshots`.

- `GET /categories`
  - Returns the taxonomy tree for filters.
  - Source table: `dim_taxonomy`.

- `GET /price-changes`
  - Query params: `supermarket`, `category`, `direction`, `limit`.
  - Source table: `fact_product_price_changes`.

### Middle Goals

- Create `api/` folder with a minimal FastAPI app.
- Use parameterized BigQuery queries only.
- Add pagination from the start.
- Add response models so the website has a stable contract.
- Add simple caching headers for read-heavy endpoints.
- Add a local `.env.example` for API config.
- Add Dockerfile and Cloud Run deployment notes.
- Add smoke tests for:
  - health endpoint.
  - products endpoint with filters.
  - category endpoint.
  - price history endpoint.

### Done When

- The API can return today's product list from gold.
- The API can return one product's price history.
- The API does not need to know about bronze, intermediate, or silver tables.

## Plan 4: Build the Simple Website

### Target

Create a small website that makes the data useful without trying to become a full product immediately.

### First Website Scope

- Product search.
- Supermarket filter.
- Category/subcategory filter.
- Product detail page.
- Price history chart.
- Price changes page.

### Middle Goals

- Pick a lightweight frontend stack.
  - Good default: Next.js or Vite + React.
- Build against the API, not directly against BigQuery.
- Create a small design system:
  - filters.
  - product rows/cards.
  - price chart.
  - loading and empty states.
- Add basic analytics only after the core pages work.
- Deploy to a simple host:
  - Cloud Run static server, Vercel, or Firebase Hosting.

### Done When

- A user can search a product, filter by supermarket/category, and open a product detail page.
- A product detail page shows current price and historical price.
- The website works using only public API endpoints.

## Plan 5: Quality, Monitoring, and Maintenance

### Target

Keep the pipeline trustworthy as more supermarkets and product rows are added.

### Middle Goals

- Add daily row-count tracking by supermarket and layer.
- Add quality tables or views for:
  - missing price.
  - missing product URL.
  - unparsed unit price.
  - taxonomy mapped to `Other`.
- Add a weekly review task for taxonomy/rules updates.
- Add retry guidance for scraper failures.
- Add a short runbook:
  - how to rerun one scraper.
  - how to reload one day into bronze.
  - how to rerun dbt for one day or one model group.
  - how to check whether the website data is fresh.

### Done When

- Failures are easy to detect.
- Bad data is visible before it reaches the website.
- Manual fixes have documented steps.

## Plan 6: AI Shopping Agent

### Target

Create an AI agent that answers questions about products and helps decide where to buy items from a shopping list.

### First Scope

- Product Q&A using the API and gold tables.
- Shopping-list parsing.
- Product matching for each list item.
- Supermarket allocation for selected supermarkets.
- Structured response for the website.

### Middle Goals

- Add API-backed product search tools.
- Parse messy shopping lists into structured items.
- Match list items to product candidates.
- Support recommendation modes:
  - cheapest.
  - fewest stores.
  - balanced.
  - preferred supermarket first.
- Return item-level recommendations, unresolved items, substitutions, and estimated basket cost.
- Add an evaluation set of common product questions and shopping lists.

### Done When

- A user can ask product questions and get answers grounded in current data.
- A user can give a shopping list and allowed supermarkets.
- The agent tells them where to buy each item and explains uncertain matches.

## Suggested Execution Order

### Sprint 1: Warehouse facts

- Finish `fact_products_today`.
- Add `fact_product_price_snapshots`.
- Add `dim_taxonomy`.
- Add tests and docs.
- Run `dbt build`.

### Sprint 2: Historical insights

- Add `fact_product_price_changes`.
- Add quality checks for today's load completeness.
- Decide final gold table contracts for API use.

### Sprint 3: Automation

- Standardize scraper upload behavior.
- Deploy or document the bronze loader.
- Add dbt runner job.
- Add a scheduled daily warehouse refresh.

### Sprint 4: API

- Create FastAPI service.
- Implement `/products`, `/categories`, and `/price-history`.
- Deploy API to Cloud Run.

### Sprint 5: Website

- Create the simple product search website.
- Add product detail and price history chart.
- Connect website to deployed API.

### Sprint 6: AI shopping agent

- Build product Q&A over the API.
- Add shopping-list parsing and product matching.
- Add supermarket allocation recommendations.
- Connect the agent to the website.

## Keep Out of Scope for Now

- User accounts.
- Personalized alerts.
- Machine learning price forecasts.
- Complex product matching across supermarkets.

These can come later, after the warehouse facts, automation, API, and first website are working end to end.

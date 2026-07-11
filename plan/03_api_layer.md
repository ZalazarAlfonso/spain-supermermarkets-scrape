# Plan 03: API Layer

## Target

Create a small read API for the website. The API should read from gold tables only.

Recommended stack:

- FastAPI
- BigQuery Python client
- Cloud Run

## Start Here

Create the smallest possible API skeleton:

- `GET /health`
- configuration for BigQuery project/dataset
- local run instructions
- Dockerfile

Then add product listing.

## Middle Goals

### Middle Goal 1: API Skeleton

Create an `api/` folder.

Minimum files:

- `api/main.py`
- `api/requirements.txt`
- `api/Dockerfile`
- `api/.env.example`
- `api/README.md`

Done when:

- `GET /health` returns OK locally.
- The service can be containerized.

### Middle Goal 2: Products Endpoint

Add `GET /products`.

Query params:

- `supermarket`
- `group`
- `category`
- `subcategory`
- `search`
- `limit`
- `cursor`

Source:

- `fact_products_today`.

Done when:

- Product search and filters work.
- Query uses BigQuery parameters.
- Response shape is stable.

### Middle Goal 3: Product Detail Endpoint

Add `GET /products/{source_product_id}`.

Source:

- `fact_products_today`.

Done when:

- The endpoint returns a single current product.
- Not-found products return a clean 404.

### Middle Goal 4: Price History Endpoint

Add `GET /products/{source_product_id}/price-history`.

Source:

- `fact_product_price_snapshots`.

Done when:

- It returns dated price points in chart-friendly order.

### Middle Goal 5: Categories and Price Changes

Add:

- `GET /categories`
- `GET /price-changes`

Sources:

- `dim_taxonomy`
- `fact_product_price_changes`

Done when:

- The website can build filters without hardcoding categories.
- The website can show recent price drops/increases.

## Build Checklist

- Use parameterized queries.
- Add response models.
- Add pagination.
- Add basic request validation.
- Add simple cache headers for read-heavy endpoints.
- Add smoke tests.
- Add Cloud Run deployment notes.

## Done When

- The API can return products, one product detail, price history, categories, and price changes.
- The website can be built without knowing anything about BigQuery.
- All API reads come from gold tables.

## Later

- API keys or rate limiting.
- CDN caching.
- Full-text search service.
- Product matching endpoints.

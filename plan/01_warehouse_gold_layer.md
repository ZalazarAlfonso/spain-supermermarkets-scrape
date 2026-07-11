# Plan 01: Warehouse Gold Layer

## Target

Create a stable gold layer that the API and website can use directly. The gold layer should answer:

- What products are available today?
- What is the latest price per product?
- How did prices change over time?
- What category tree should the website use for filters?

## Start Here

Finish `fact_products_today` first. It is already started, and it is the most important table for the API.

Recommended first session:

1. Confirm its grain: one row per `source_product_id` for the latest scrape date.
2. Decide the API-facing columns.
3. Add tests in `gold.yml`.
4. Run a focused dbt build for the gold model.

## Middle Goals

### Middle Goal 1: Current Products Fact

Finish `fact_products_today`.

Expected columns:

- `source_product_id`
- `supermarket`
- `product_url`
- `product_name`
- `brand_raw`
- `group_std`
- `category_std`
- `subcategory_std`
- `price_value`
- `unit_price_std_value`
- `unit_price_std_unit`
- `offer_flag`

Done when:

- There is one row per current product.
- Required API fields are not null where they must be usable.
- `gold.yml` documents the model and tests important columns.

### Middle Goal 2: Historical Price Fact

Add `fact_product_price_snapshots`.

Expected grain:

- one row per `source_product_id`, `supermarket`, and `scrape_date`.

Use it for:

- product detail price charts.
- historical analysis.
- future basket/price comparison work.

Done when:

- Historical prices can be queried without touching silver models.
- Duplicate product/date rows are tested.

### Middle Goal 3: Product Dimension

Add `dim_products`.

Purpose:

- Keep product identity and descriptive metadata in one place.

Expected grain:

- one row per `source_product_id`.

Done when:

- Product metadata can be joined to facts through `source_product_id`.
- It includes supermarket, product URL, product name, and brand.

### Middle Goal 4: Taxonomy Dimension

Add `dim_taxonomy`.

Purpose:

- Give the website a clean filter tree.

Source:

- `standard_taxonomy_v1`.

Done when:

- The API can build category filters from this table.
- Sort/order fields are available.

### Middle Goal 5: Price Changes Fact

Add `fact_product_price_changes`.

Expected columns:

- `source_product_id`
- `supermarket`
- `scrape_date`
- `price_value`
- `previous_scrape_date`
- `previous_price_value`
- `price_change_value`
- `price_change_pct`
- `price_change_direction`

Done when:

- You can query price drops and increases directly from gold.

## Build Checklist

- Add missing gold SQL models.
- Add model documentation in `dbt/supermarket_dwh/models/gold/gold.yml`.
- Add unique and not-null tests for each model's grain.
- Add accepted values tests for supermarket.
- Add taxonomy consistency tests where useful.
- Run `dbt build --select gold+` or the equivalent selected dbt command.

## Done When

- Gold tables cover current products, price history, product metadata, taxonomy filters, and price changes.
- The API plan can be implemented without querying silver/intermediate models directly.
- A future website can search, filter, show product detail, and show price history from gold/API data.

## Later

- Cross-supermarket product matching.
- Basket-level facts.
- Forecasting tables.
- User-specific alert tables.

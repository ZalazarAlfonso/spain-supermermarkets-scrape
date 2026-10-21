# Data fixes runbook (October 2026)

What to run, in order, to ship the `data-fixes/*` branches. Every command
targets project `lab-spanish-smarkts-scraper`.

## Branches

| Branch | Base | Contents |
|--------|------|----------|
| `data-fixes/p1-unblock-gold` | `dwh-case-size-and-incremental` | Taxonomy fallback and rules; the unmapped test no longer blocks gold |
| `data-fixes/p2-current-catalogue` | p1 | Per-supermarket current catalogue, `supermarket_freshness`, softer readiness gate |
| `data-fixes/p4-gold-contract` | p2 | `fact_product_price_changes`, price flags, search text, `dim_products` lifecycle, `dim_taxonomy` rename |
| `data-fixes/p5-identity-bridge` | p4 | Product identity across URL changes (not used by gold yet) |
| `data-fixes/p3-scrapers` | `dwh-case-size-and-incremental` | Mercadona shelf price, Dia unit capture, Alcampo nav labels |
| `data-fixes/p6-loader-cleanup` | `dwh-case-size-and-incremental` | Loader inserts by column name, no staging leaks; this runbook; CLAUDE.md |

## 1. Recover the Dia gap (before deleting any staging table)

Dia files for 2026-08-26 to 2026-09-15 (19 days; 09-02 and 09-13 never
arrived) failed to load, but each day's rows still sit in `_stg_*` tables.
This inserts one copy per day, only for days bronze does not already have:

```sql
INSERT INTO `lab-spanish-smarkts-scraper.dwh_bronze_dev.bronze_dia_products_p`
  (date, product, brand, price, price_per_unit, offer, category, subcategory, product_url)
SELECT date, product, SAFE_CAST(brand AS FLOAT64), price, price_per_unit, offer, category, subcategory, product_url
FROM `lab-spanish-smarkts-scraper.dwh_bronze_dev._stg_bronze_dia_products_p_*`
WHERE _TABLE_SUFFIX IN (
  '20260826_3e7800a093','20260827_215d30b878','20260828_587766f433','20260829_ff440a6966',
  '20260830_2a6bfa9e60','20260831_a370befb43','20260901_d2afd9db5f','20260903_255db21791',
  '20260904_da6cdf2f64','20260905_1fde5499c3','20260906_b0960048a7','20260907_3dcfea7601',
  '20260908_6113a9c431','20260909_b92481a97b','20260910_44f405c2fc','20260911_063c07a6d2',
  '20260912_fe79a96661','20260914_f58370a56e','20260915_83ccd90b5e')
  AND date = PARSE_DATE('%Y%m%d', SUBSTR(_TABLE_SUFFIX, 1, 8))
  AND date NOT IN (
    SELECT DISTINCT date FROM `lab-spanish-smarkts-scraper.dwh_bronze_dev.bronze_dia_products_p`
    WHERE date BETWEEN '2026-08-26' AND '2026-09-15');
```

Expect about 95,000 rows. `brand` loads as NULL, like the rest of Dia's
bronze history (the column is FLOAT64; see Follow-ups). Undo with
`DELETE ... WHERE date BETWEEN '2026-08-26' AND '2026-09-15'`.

## 2. Delete the leaked staging tables

About 4,900 `_stg_bronze_*` tables (Dia's alone hold 18M rows). List them first:

```sql
SELECT table_id, row_count, TIMESTAMP_MILLIS(creation_time) AS created
FROM `lab-spanish-smarkts-scraper.dwh_bronze_dev.__TABLES__`
WHERE STARTS_WITH(table_id, '_stg_bronze_')
ORDER BY creation_time;
```

Then generate the drops and run them (cannot be undone after BigQuery's
7-day time-travel window):

```bash
bq query --project_id=lab-spanish-smarkts-scraper --use_legacy_sql=false --format=csv --max_rows=10000 \
  "SELECT table_id FROM \`lab-spanish-smarkts-scraper.dwh_bronze_dev.__TABLES__\` WHERE STARTS_WITH(table_id, '_stg_bronze_')" \
  | tail -n +2 | while read -r t; do bq rm -f -t "lab-spanish-smarkts-scraper:dwh_bronze_dev.$t"; done
```

## 3. Deploy

1. Bronze loader (p6): redeploy `ingestion/bq_event_loader` as before.
2. Scrapers (p3): rebuild the Mercadona, Dia and Alcampo images with their
   `cloudbuild.*.scrape_daily.yaml` and push them as `:latest`. The VMs pull
   `:latest` on their next start; Mercadona is a Cloud Run job.
3. dbt runner (p1, p2, p4, p5 merged): rebuild the `dbt-runner-dev` image
   (it bundles `scripts/check_bronze_readiness.py` and the dbt project).

## 4. Full refresh

The rules, taxonomy fallback, `price_is_shelf_price` and the new columns
only reach history on a full rebuild:

```bash
gcloud run jobs execute dbt-runner-dev --region=europe-southwest1 \
  --project=lab-spanish-smarkts-scraper --update-env-vars=DBT_FULL_REFRESH=true
```

Then check:

```sql
SELECT * FROM `lab-spanish-smarkts-scraper.dwh_gold_dev.supermarket_freshness`;
SELECT * FROM `lab-spanish-smarkts-scraper.dwh_silver_dev.qa_unmapped_products`;
```

## 5. Review the identity links, then wire them in

```sql
SELECT * FROM `lab-spanish-smarkts-scraper.dwh_silver_dev.qa_product_identity_links_sample`;
```

If the pairs are the same product, partition `fact_product_price_changes`
by `silver_product_identity.canonical_product_id` instead of
`source_product_id`, and add `canonical_product_id` to `dim_products`.

## Not fixed: Carrefour and Alcampo are being blocked

The VM logs show Carrefour answering 403 to both plain requests and the
Playwright fallback, and Alcampo pages flagged `blocked=True`. This is the
sites' bot protection, not a parsing bug, and it was left alone on purpose.
The warehouse now copes with it (each supermarket's catalogue comes from its
last complete scrape and is flagged stale), but fresh data for these two
needs a sanctioned route: an official feed or API, or permission from the
retailer.

## Follow-ups

- `bronze_dia_products_p.brand` and `bronze_carrefour_products_p.brand` are
  FLOAT64 because the tables were created from files where brand was all
  null. Changing them to STRING means rewriting the tables
  (`CREATE OR REPLACE TABLE ... AS SELECT * REPLACE (CAST(brand AS STRING) AS brand)`,
  keeping partitioning and clustering).
- Mercadona's API also returns `unit_size`/`size_format` (pack size) and
  `previous_unit_price` (regular price, so offers can show a discount).
  Capturing them needs new bronze columns; the loader now loads by column
  name, so add the columns to bronze first, then emit them.

# Guide 01: Introductory Guide to Tests in dbt

This guide introduces dbt tests using the supermarket warehouse project as the running example. By the end, you should understand what dbt tests are, where they live, how to write the common ones, when to write custom SQL tests, and how to run and debug them.

## 1. What a dbt Test Is

A dbt test is a data quality check. It asks a question like:

- Is this column always present?
- Is this identifier unique?
- Does this column only contain allowed values?
- Do standardized categories exist in the taxonomy seed?
- Do business rules contradict each other?

The important mental model is:

> A dbt test fails when its query returns rows.

For example, a `not_null` test on `product_url` fails if dbt finds rows where `product_url is null`.

This is different from a Python unit test. A dbt test does not usually test application code directly. It tests the data produced by your dbt models.

## 2. Why Tests Matter in This Project

Your pipeline moves supermarket data through layers:

```text
bronze raw tables
  -> staging models
  -> intermediate models
  -> silver trusted product models
  -> gold fact tables
```

Each layer has different risks.

In staging, tests should catch broken raw data assumptions, such as missing source URLs or dates.

In silver, tests should catch trusted-model problems, such as missing product IDs, invalid supermarket names, null prices, or duplicate snapshots.

In gold, tests should protect user-facing tables, such as `fact_products_today`, from incomplete or duplicated records.

In seeds, tests should protect reference data, such as standard category taxonomy and mapping rules.

## 3. The Two Main Types of dbt Tests

dbt has two common test styles:

1. Generic tests
2. Singular tests

### Generic Tests

Generic tests are reusable tests configured in YAML files.

Your project already uses these in files like:

- `dbt/supermarket_dwh/models/silver/silver.yml`
- `dbt/supermarket_dwh/models/gold/gold.yml`
- `dbt/supermarket_dwh/seeds/seeds.yml`
- `dbt/supermarket_dwh/models/staging/sources.yml`

Example:

```yaml
columns:
  - name: product_url
    data_tests:
      - not_null
```

This means:

> For this model, `product_url` should never be null.

### Singular Tests

Singular tests are custom SQL files in the `tests/` directory.

Your project already has examples:

- `dbt/supermarket_dwh/tests/assert_standard_taxonomy_unique_pairs.sql`
- `dbt/supermarket_dwh/tests/assert_standard_rules_group_consistency.sql`
- `dbt/supermarket_dwh/tests/assert_standard_rules_required_fields_not_blank.sql`
- `dbt/supermarket_dwh/tests/assert_silver_product_standardized_taxonomy_membership.sql`

A singular test is just a SQL query that returns bad rows.

Example pattern:

```sql
select *
from {{ ref('some_model') }}
where bad_condition_is_true
```

If the query returns zero rows, the test passes. If it returns one or more rows, the test fails.

## 4. Built-In Generic Tests You Should Know First

dbt ships with several generic tests. The most useful starter set is:

- `not_null`
- `unique`
- `accepted_values`
- `relationships`

### `not_null`

Use `not_null` when a column is required for downstream logic.

Good candidates in this project:

- product identifiers
- product URLs
- scrape dates
- supermarket names
- standardized category fields
- prices in trusted or gold models

Example from your gold model:

```yaml
- name: fact_products_today
  columns:
    - name: product_url
      data_tests:
        - not_null
```

This test should exist because a product fact row without a URL is not very useful for downstream analysis.

### `unique`

Use `unique` when a column should identify one row.

Example:

```yaml
- name: silver_product_snapshots
  columns:
    - name: product_snapshot_id
      data_tests:
        - not_null
        - unique
```

This says every product snapshot must have an ID, and no two rows should share the same snapshot ID.

Be careful: not every ID-like column is unique in every table. For example, `source_product_id` may be unique in a current-products table but not in a historical snapshots table, because the same product can appear on multiple scrape dates.

### `accepted_values`

Use `accepted_values` when a column should only contain a fixed set of values.

Example from your project:

```yaml
- name: supermarket
  data_tests:
    - not_null
    - accepted_values:
        arguments:
          values: ['alcampo', 'carrefour', 'dia', 'mercadona']
```

This catches typos and unexpected source names like `al_campo`, `Carrefour`, or `mercadonna`.

### `relationships`

Use `relationships` when a column in one model should exist in another model.

For example, if a model had a `category_std` column and every category had to exist in `standard_taxonomy_v1`, you could write:

```yaml
- name: category_std
  data_tests:
    - relationships:
        arguments:
          to: ref('standard_taxonomy_v1')
          field: category_std
```

However, your taxonomy case has special logic because `Other` is allowed and because group, category, and subcategory are checked together. That is why your project uses a custom singular test instead:

```text
tests/assert_silver_product_standardized_taxonomy_membership.sql
```

## 5. Where Tests Live in This Project

Use this structure:

```text
dbt/supermarket_dwh/
  models/
    staging/
      sources.yml
    silver/
      silver.yml
    gold/
      gold.yml
  seeds/
    seeds.yml
  tests/
    assert_*.sql
```

### Source Tests

Source tests live in `models/staging/sources.yml`.

They test raw BigQuery source tables, such as:

```yaml
sources:
  - name: bronze
    schema: dwh_bronze_dev
    tables:
      - name: bronze_carrefour_products_p
        columns:
          - name: product_url
            data_tests:
              - not_null
          - name: date
            data_tests:
              - not_null
```

Use source tests for minimum raw-data expectations.

### Model Tests

Model tests live next to model groups.

Silver tests are in:

```text
dbt/supermarket_dwh/models/silver/silver.yml
```

Gold tests are in:

```text
dbt/supermarket_dwh/models/gold/gold.yml
```

Use model tests for data guarantees that downstream models and users depend on.

### Seed Tests

Seed tests live in:

```text
dbt/supermarket_dwh/seeds/seeds.yml
```

Use seed tests for manually maintained reference data, especially when mistakes in a CSV could silently affect many products.

### Singular SQL Tests

Singular tests live in:

```text
dbt/supermarket_dwh/tests/
```

Use singular tests when the rule is too specific for a simple YAML test.

## 6. How to Choose What to Test

Start with these questions:

1. Would a null value break downstream logic?
2. Should this column uniquely identify rows?
3. Should this column only contain known values?
4. Does this model depend on a reference table or seed?
5. Is there business logic that must always remain true?
6. Would a dashboard, API, or analysis become misleading if this failed?

Good tests are not about testing every column. They protect assumptions.

## 7. Recommended Tests by Layer

### Bronze Sources

Test raw source columns that must exist for ingestion and deduplication:

- `product_url` should be not null.
- `date` should be not null.

Possible next tests:

```yaml
- name: product_url
  data_tests:
    - not_null

- name: date
  data_tests:
    - not_null
```

Avoid being too strict in bronze. Raw data can be messy, and bronze should preserve that mess for later cleaning.

### Staging Models

Staging models standardize source shape. Good tests include:

- `source_product_id` is not null.
- `product_snapshot_id` is not null.
- `supermarket` is not null and accepted.
- `product_url` is not null.
- `scrape_date` is not null.

Example:

```yaml
- name: stg_carrefour_products
  columns:
    - name: source_product_id
      data_tests:
        - not_null
    - name: product_snapshot_id
      data_tests:
        - not_null
    - name: supermarket
      data_tests:
        - not_null
        - accepted_values:
            arguments:
              values: ['carrefour']
```

### Intermediate Models

Intermediate models are transformation workbenches. Test the assumptions that other models rely on:

- parsed numeric prices are not null when the row is considered valid
- unit parsing produces expected unit families
- quality audit flags are in expected ranges

Example idea:

```yaml
- name: int_products_quality_audit
  columns:
    - name: quality_status
      data_tests:
        - accepted_values:
            arguments:
              values: ['valid', 'invalid']
```

Only add this if the column exists in the model.

### Silver Models

Silver is trusted data. Be stricter here.

Existing good tests:

- `product_snapshot_id` not null and unique in `silver_product_snapshots`
- `source_product_id` not null
- `supermarket` accepted values
- `scrape_date` not null
- `product_url` not null
- `product_name` not null
- `price_value` not null
- standardized taxonomy fields not null

For `silver_product_standardized`, good tests are:

```yaml
- name: group_std
  data_tests:
    - not_null

- name: category_std
  data_tests:
    - not_null

- name: subcategory_std
  data_tests:
    - not_null
```

Then use a singular test to verify the standardized values belong to your taxonomy.

### Gold Models

Gold is user-facing. Tests here should protect analytics and app consumption.

For `fact_products_today`, good tests include:

- `source_product_id` not null and unique
- `supermarket` not null and accepted
- `product_url` not null
- `product_name` not null
- `group_std`, `category_std`, `subcategory_std` not null
- `price_value` not null

For `fact_product_price_snapshots`, likely tests include:

- snapshot ID not null and unique, if the model has one row per snapshot
- source product ID not null
- supermarket accepted values
- scrape date not null
- price value not null

Always match tests to the actual model grain.

## 8. Understanding Model Grain Before Testing Uniqueness

Before adding a `unique` test, ask:

> What is one row in this model?

Examples:

`silver_product_snapshots`

One row is probably one supermarket product on one scrape date. `product_snapshot_id` should be unique. `source_product_id` should not be unique because the same product can appear across dates.

`silver_product_current`

One row is probably the latest row per supermarket product. `source_product_id` may be unique here if the model truly keeps one row per source product.

`fact_products_today`

One row is probably one current product. `source_product_id` should be unique if the table keeps only the latest date.

This is the most common testing mistake in dbt: adding `unique` to a column that is unique in one layer but not in another.

## 9. How to Write a Generic Test

Open the relevant YAML file.

For silver models:

```text
dbt/supermarket_dwh/models/silver/silver.yml
```

Add or update a column:

```yaml
models:
  - name: silver_product_current
    columns:
      - name: product_url
        data_tests:
          - not_null
```

For dbt 1.11, your project is using the newer `data_tests` key. Keep using that style.

Older dbt examples often use:

```yaml
tests:
  - not_null
```

Prefer `data_tests` in this project for consistency.

## 10. How to Write a Singular SQL Test

Create a SQL file in:

```text
dbt/supermarket_dwh/tests/
```

The file name should explain the assertion:

```text
assert_fact_products_today_prices_positive.sql
```

The query should return rows that violate the rule:

```sql
select
    source_product_id,
    supermarket,
    product_name,
    price_value
from {{ ref('fact_products_today') }}
where price_value <= 0
```

If there are no rows with `price_value <= 0`, the test passes.

If there are rows, the test fails and shows the bad records.

Good singular test names usually start with:

```text
assert_
```

This makes it obvious that the SQL file is a data assertion, not a model.

## 11. Custom Tests Already in This Project

### `assert_standard_taxonomy_unique_pairs.sql`

Purpose:

Ensures the taxonomy seed does not contain duplicate `(category_std, subcategory_std)` pairs.

Why it matters:

Duplicate taxonomy pairs can make standardized category joins ambiguous.

### `assert_standard_rules_required_fields_not_blank.sql`

Purpose:

Ensures important rule fields are not blank strings.

Why it matters:

CSV seeds often contain empty strings instead of actual nulls. A `not_null` test does not catch empty strings, so this singular test catches a different class of problem.

### `assert_standard_rules_group_consistency.sql`

Purpose:

Ensures rows in a rule group do not disagree on target or priority.

Why it matters:

Rows with the same `rule_id` are treated as one combined rule. If rows in that group point to conflicting targets or priorities, the standardization logic becomes unreliable.

### `assert_silver_product_standardized_taxonomy_membership.sql`

Purpose:

Ensures standardized groups, categories, and subcategories in `silver_product_standardized` exist in `standard_taxonomy_v1`, except for the allowed fallback value `Other`.

Why it matters:

This protects the main promise of the standardized silver model: category values should be controlled, not arbitrary strings.

## 12. How to Run Tests

Run commands from the dbt project directory:

```bash
cd dbt/supermarket_dwh
```

Run all tests:

```bash
../../.venv/bin/dbt test
```

Run tests for one model:

```bash
../../.venv/bin/dbt test --select fact_products_today
```

Run a model and its tests:

```bash
../../.venv/bin/dbt build --select fact_products_today
```

Run a model, its parents, and its tests:

```bash
../../.venv/bin/dbt build --select +fact_products_today
```

Run only singular tests:

```bash
../../.venv/bin/dbt test --select test_type:singular
```

Run only generic tests:

```bash
../../.venv/bin/dbt test --select test_type:generic
```

Run one specific test by name:

```bash
../../.venv/bin/dbt test --select assert_standard_taxonomy_unique_pairs
```

## 13. How to Read a Test Failure

When a test fails, dbt tells you:

- the test name
- the model or seed being tested
- the number of failing rows
- the compiled SQL file path

The compiled SQL is useful because it shows the actual BigQuery query dbt ran.

Compiled test SQL is usually under:

```text
dbt/supermarket_dwh/target/compiled/
```

For a failing test, open the compiled SQL and run it in BigQuery. The returned rows are the bad rows.

## 14. Debugging Workflow

Use this workflow when a test fails:

1. Read the test name.
2. Find the YAML or SQL file that defines it.
3. Open the compiled SQL in `target/compiled/`.
4. Run the compiled SQL in BigQuery.
5. Inspect the rows returned.
6. Decide whether the data is wrong or the test assumption is wrong.
7. Fix the source data, model SQL, seed CSV, or test definition.
8. Rerun the test.

Do not automatically weaken a test because it fails. A failing test is often doing its job.

## 15. Common Mistakes

### Testing the Wrong Grain

Adding `unique` to `source_product_id` in a historical snapshots table is probably wrong, because one product can have many snapshots.

### Testing Raw Data Too Strictly

Bronze data can be messy. Test the minimum requirements there, then enforce stronger expectations in silver and gold.

### Confusing Nulls and Blank Strings

`not_null` catches nulls. It does not catch empty strings like `''`.

Use a singular test for blank strings:

```sql
select *
from {{ ref('standard_rules_v1') }}
where trim(target) = ''
```

### Forgetting Accepted Values

Fields like `supermarket`, `match_type`, and rule `level` should usually have `accepted_values` tests because they drive logic.

### Writing a Singular Test That Returns Good Rows

Singular tests should return bad rows. If your query returns valid rows, dbt will treat those valid rows as failures.

## 16. Practical Checklist for Guide 1 Tests

Use this checklist when adding tests for a new model or seed.

### Required Identity Tests

- Does the model have a row ID?
- Should that row ID be `not_null`?
- Should that row ID be `unique`?

### Required Dimension Tests

- Is `supermarket` always present?
- Is `supermarket` limited to `alcampo`, `carrefour`, `dia`, and `mercadona`?
- Are standardized category fields present?
- Should standardized fields exist in the taxonomy?

### Required Measure Tests

- Is `price_value` present in trusted models?
- Should price be greater than zero?
- Should unit prices be greater than zero when present?

### Required Date Tests

- Is `scrape_date` present?
- Should the model contain only the latest date?
- Should a gold "today" fact table contain one row per product?

### Required Seed Tests

- Are key seed columns not null?
- Are allowed-value columns constrained?
- Are there duplicate taxonomy rows?
- Are blank strings forbidden in required rule fields?
- Are grouped rules internally consistent?

## 17. Suggested Next Tests for This Project

These are good next candidates, assuming the model columns support them.

### Positive Prices in Gold

```sql
-- dbt/supermarket_dwh/tests/assert_fact_products_today_prices_positive.sql
select
    source_product_id,
    supermarket,
    product_name,
    price_value
from {{ ref('fact_products_today') }}
where price_value <= 0
```

### Current Products Are Unique

If `silver_product_current` has one row per product:

```yaml
- name: silver_product_current
  columns:
    - name: source_product_id
      data_tests:
        - not_null
        - unique
```

### Gold Table Uses Only Latest Scrape Date

If `fact_products_today` should contain only one scrape date and the column exists:

```sql
-- dbt/supermarket_dwh/tests/assert_fact_products_today_single_scrape_date.sql
select
    scrape_date
from {{ ref('fact_products_today') }}
group by scrape_date
having count(*) > 0
qualify count(*) over () > 1
```

If `fact_products_today` does not include `scrape_date`, either add it to the model or skip this test.

### No Blank Product Names

```sql
-- dbt/supermarket_dwh/tests/assert_fact_products_today_product_name_not_blank.sql
select
    source_product_id,
    supermarket,
    product_url,
    product_name
from {{ ref('fact_products_today') }}
where trim(product_name) = ''
```

## 18. Suggested Test Standards

Use these standards for this repository:

- Put generic model tests in the closest model YAML file.
- Put source tests in `models/staging/sources.yml`.
- Put seed tests in `seeds/seeds.yml`.
- Put custom SQL assertions in `tests/`.
- Name singular tests with `assert_`.
- Make singular tests return bad rows only.
- Add `not_null` before `unique` on identifiers.
- Be strictest in silver and gold.
- Be careful with uniqueness in historical tables.
- Prefer explicit accepted values for low-cardinality business fields.

## 19. Minimal Example: Adding a New Test

Suppose you want to ensure `fact_products_today.price_value` is always present.

Open:

```text
dbt/supermarket_dwh/models/gold/gold.yml
```

Add:

```yaml
- name: price_value
  data_tests:
    - not_null
```

Then run:

```bash
cd dbt/supermarket_dwh
../../.venv/bin/dbt test --select fact_products_today
```

If it passes, the assumption is protected.

If it fails, inspect the returned rows and decide whether the transformation logic or raw data needs fixing.

## 20. Final Mental Model

dbt tests are executable assumptions about your warehouse.

Use generic tests for common assumptions:

- required values
- uniqueness
- accepted values
- simple relationships

Use singular tests for business-specific rules:

- taxonomy membership
- rule consistency
- blank strings
- positive prices
- latest-date guarantees

The goal is not to create as many tests as possible. The goal is to protect the assumptions that would make the warehouse misleading if they broke.

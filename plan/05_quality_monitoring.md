# Plan 05: Quality and Monitoring

## Target

Make the pipeline trustworthy. This plan should run alongside the others, not only after everything else is done.

Focus on:

- detecting bad or missing data.
- knowing when today's data is ready.
- making recovery steps obvious.

## Start Here

Add a simple daily completeness check.

It should answer:

- Did every supermarket load today?
- How many rows loaded per supermarket?
- Is any supermarket at zero rows?
- What is the latest available date per supermarket?

## Middle Goals

### Middle Goal 1: Bronze Completeness

Add a query, dbt model, or documented BigQuery snippet for bronze load status.

Done when:

- You can quickly see today's row counts by supermarket.
- Missing data is obvious.

### Middle Goal 2: Silver Quality

Track rows filtered or flagged by quality rules.

Useful categories:

- missing product URL.
- missing product name.
- missing raw price.
- unparsed price.
- unparsed unit price.

Done when:

- You can see whether scraper changes are damaging downstream quality.

### Middle Goal 3: Taxonomy Coverage

Monitor standardization coverage.

Useful metrics:

- percent mapped to `Other`.
- `Other` rows by supermarket.
- top raw categories/subcategories that need rules.

Done when:

- Taxonomy cleanup becomes a weekly small task instead of a vague worry.

### Middle Goal 4: Alerts

Add basic alerts.

Start with:

- scraper job failed.
- bronze loader failed.
- dbt build failed.
- zero rows loaded for a supermarket.

Done when:

- Pipeline failures are visible without opening every service manually.

### Middle Goal 5: Runbook

Create a short runbook.

It should explain:

- how to rerun one scraper.
- how to rerun target discovery.
- how to reload one date into bronze.
- how to rerun dbt.
- how to check whether the website data is fresh.

Done when:

- A tired future version of you can fix the common problems without rediscovering the whole system.

## Build Checklist

- Add daily row-count checks.
- Add quality summary models or queries.
- Add taxonomy coverage checks.
- Add alerting hooks.
- Add `plan/runbook.md` or a docs runbook.
- Add a weekly taxonomy review checklist.

## Done When

- You can tell whether today's data is complete.
- You can see the main quality problems.
- You have documented recovery steps.
- Bad data is less likely to quietly reach the website.

## Later

- Data freshness dashboard.
- Historical quality trends.
- Automated taxonomy rule suggestions.
- Backfill validation reports.

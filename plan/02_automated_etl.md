# Plan 02: Automated ETL

## Target

Make the data pipeline run without manual work:

1. Category discovery updates target JSON files.
2. Daily scrapers write Parquet to GCS.
3. GCS events load Parquet into BigQuery bronze.
4. dbt refreshes intermediate, silver, and gold.
5. Failures are visible.

## Start Here

Standardize scraper upload behavior. This is the smallest automation blocker because the rest of the pipeline depends on predictable GCS files.

Recommended first session:

1. Check all four daily scrapers.
2. Decide whether uploads are controlled by `--upload-to-gcs`, `UPLOAD_TO_GCS`, or both.
3. Make behavior consistent.
4. Document the convention.

## Middle Goals

### Middle Goal 1: Consistent Raw Uploads

Make all scrapers follow the same GCS upload convention.

Done when:

- Carrefour, Mercadona, Dia, and Alcampo all write Parquet to predictable paths.
- Upload behavior is documented.
- Local smoke runs can avoid uploading.

### Middle Goal 2: Bronze Loader Confidence

Validate `ingestion/bq_event_loader`.

Done when:

- It accepts all expected supermarket paths.
- It maps each supermarket prefix to the correct bronze table.
- It handles repeat loads for the same day by replacing that day's rows.
- It logs enough context to debug failed loads.

### Middle Goal 3: Pipeline Readiness Check

Add a query or dbt model that checks whether today's data is complete.

It should show:

- row count by supermarket.
- latest loaded date by supermarket.
- missing supermarket loads.
- suspicious zero-row loads.

Done when:

- You can tell whether dbt should run safely for today's data.

### Middle Goal 4: dbt Runner

Create a repeatable dbt execution job.

Good first version:

- a container or Cloud Run Job that runs `dbt seed` and `dbt build`.

Done when:

- dbt can run from a clean environment using secrets/env config.
- logs show model failures clearly.

### Middle Goal 5: Scheduling

Schedule the work.

Recommended first version:

- weekly Cloud Scheduler jobs for target discovery.
- daily Cloud Scheduler jobs for scrapers.
- daily scheduled dbt job after scraper jobs usually finish.

Better later version:

- trigger dbt only after all expected bronze loads are present.

Done when:

- The full daily refresh can happen without local commands.

## Build Checklist

- Standardize GCS upload flags/env vars.
- Confirm Parquet schema matches BigQuery bronze schema.
- Deploy or document the Eventarc/GCS loader.
- Add a dbt runner Dockerfile or job definition.
- Add Cloud Scheduler commands/config.
- Add basic alerting for failed jobs and zero-row loads.
- Add a manual rerun procedure.

## Done When

- A daily scrape lands in GCS.
- The GCS load reaches BigQuery bronze.
- dbt updates intermediate, silver, and gold.
- You have one clear place to check whether today's warehouse is ready.

## Later

- Orchestrate with Workflows.
- Add dependency-aware dbt triggering.
- Add richer observability dashboards.
- Add backfill tooling for date ranges.

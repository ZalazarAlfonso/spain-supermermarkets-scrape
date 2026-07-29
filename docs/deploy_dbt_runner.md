# Deploy: daily dbt runner on Cloud Run Jobs

Runs the warehouse build every day after the scrapers land Bronze, with no dbt Cloud
subscription. Reuses the infrastructure the scrapers already have.

> `gcr.io` (Container Registry) was shut down in March 2025 — this uses Artifact
> Registry.

## What already exists (verified 2026-07-29)

Nothing here needs creating:

| Thing | Value |
|---|---|
| Artifact Registry repo | `scrapers` in `europe-southwest1` |
| dbt service account | `dbt-bigquery-dev@lab-spanish-smarkts-scraper.iam.gserviceaccount.com` — already has `bigquery.dataEditor`, `dataViewer`, `jobUser`, `readSessionUser` |
| Scheduler caller SA | `scraper-scheduler-sa@lab-spanish-smarkts-scraper.iam.gserviceaccount.com` |
| APIs enabled | `run`, `cloudbuild`, `artifactregistry`, `cloudscheduler` |

Two gotchas specific to this project:

- **`gcloud`'s default project is `cyber-analyzer-490508`, not the warehouse
  project.** Every command below passes `--project` explicitly. Don't drop it.
- **Cloud Scheduler is not available in `europe-southwest1`.** The existing scraper
  trigger lives in `europe-west1` and calls across regions into the Cloud Run job.
  Do the same.

## Variables

```bash
export PROJECT_ID=lab-spanish-smarkts-scraper
export REGION=europe-southwest1
export SCHED_REGION=europe-west1
export IMAGE_URI="$REGION-docker.pkg.dev/$PROJECT_ID/scrapers/dbt-runner:latest"
export DBT_SA="dbt-bigquery-dev@$PROJECT_ID.iam.gserviceaccount.com"
export SCHED_SA="scraper-scheduler-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

## 1. Build the image

Build context is the repo root — the Dockerfile copies both `dbt/supermarket_dwh`
and `scripts/check_bronze_readiness.py`.

```bash
gcloud builds submit \
  --config=cloudbuild.dbt.yaml \
  --substitutions=_IMAGE_URI="$IMAGE_URI" \
  --project="$PROJECT_ID" .
```

## 2. Create the Cloud Run Job

The container authenticates with `method: oauth`, which resolves to the attached
service account via ADC — no keyfile in the image.

```bash
gcloud run jobs create dbt-runner-dev \
  --image="$IMAGE_URI" \
  --region="$REGION" \
  --service-account="$DBT_SA" \
  --cpu=1 \
  --memory=2Gi \
  --max-retries=1 \
  --task-timeout=3600s \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,DBT_DATASET=dwh_silver_dev,DBT_LOCATION=$REGION,BRONZE_DATASET=dwh_bronze_dev,DBT_THREADS=4" \
  --project="$PROJECT_ID"
```

Run it once by hand before scheduling anything:

```bash
gcloud run jobs execute dbt-runner-dev \
  --region="$REGION" --project="$PROJECT_ID" --wait
```

After a later image rebuild:

```bash
gcloud run jobs update dbt-runner-dev --image="$IMAGE_URI" \
  --region="$REGION" --project="$PROJECT_ID"
```

## 3. Let the scheduler SA trigger the job

```bash
gcloud run jobs add-iam-policy-binding dbt-runner-dev \
  --member="serviceAccount:$SCHED_SA" \
  --role="roles/run.invoker" \
  --region="$REGION" --project="$PROJECT_ID"
```

## 4. Schedule it

The scrapers fire at 06:00 Europe/Madrid. 07:30 leaves them room to finish; the
readiness check is the real guard and aborts rather than building a partial Bronze.

```bash
gcloud scheduler jobs create http dbt-runner-daily-dev \
  --location="$SCHED_REGION" \
  --schedule="30 7 * * *" \
  --time-zone="Europe/Madrid" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/dbt-runner-dev:run" \
  --http-method=POST \
  --oauth-service-account-email="$SCHED_SA" \
  --attempt-deadline=180s \
  --project="$PROJECT_ID"
```

Note the region asymmetry: `--location` is `europe-west1` (scheduler), the URI
points at `europe-southwest1` (the job).

Trigger it manually to confirm the wiring:

```bash
gcloud scheduler jobs run dbt-runner-daily-dev \
  --location="$SCHED_REGION" --project="$PROJECT_ID"
```

## Logs

```bash
gcloud run jobs executions list --job=dbt-runner-dev \
  --region="$REGION" --project="$PROJECT_ID" --limit=5

gcloud logging read \
  'resource.type=cloud_run_job AND resource.labels.job_name=dbt-runner-dev' \
  --project="$PROJECT_ID" --limit=50 --freshness=1d
```

## Entrypoint knobs

| Env var | Default | Effect |
|---|---|---|
| `DBT_SELECT` | *(unset)* | Unset builds the whole project. Set only for scoped ad-hoc runs — a downstream-only selector like `silver_product_standardized+` leaves the incremental intermediates stale. |
| `DBT_FULL_REFRESH` | `false` | `true` appends `--full-refresh`. Needed whenever model logic or a seed changes. |
| `TARGET_DATE` | *(unset)* | Pins the readiness check to a specific date instead of today. |
| `BRONZE_MIN_ROWS` | `1` | Minimum rows per supermarket for Bronze to count as ready. |
| `DBT_THREADS` | `4` | dbt concurrency. |

### Full-refresh after a logic or seed change

Models in the `scrape_date` lineage are incremental, so a changed regex or a new
taxonomy rule only affects newly built partitions. Backfill history with:

```bash
gcloud run jobs execute dbt-runner-dev \
  --region="$REGION" --project="$PROJECT_ID" --wait \
  --update-env-vars="DBT_FULL_REFRESH=true"
```

`--update-env-vars` on `execute` applies to that execution only, so the next
scheduled run goes back to the incremental path on its own.

Better still, wire this into deployment: if a commit touches
`dbt/supermarket_dwh/models/` or `dbt/supermarket_dwh/seeds/`, run the full-refresh
execution instead of the plain one.

## Cost

At ~30k new rows/day the nightly incremental build scans well under 1 GiB; the tests
dominate at roughly 5-10 GiB. Cloud Run Jobs (1 vCPU / 2 GiB for a few minutes) and
Cloud Scheduler both sit inside their free tiers. Expect the nightly run to be
effectively free, with occasional `--full-refresh` runs (~0.7 TiB each) as the only
meaningful BigQuery cost.

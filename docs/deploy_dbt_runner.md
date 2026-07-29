# Deploy: daily dbt runner on Cloud Run Jobs

Runs the warehouse build every day after the scrapers land Bronze, with no dbt Cloud
subscription. Everything below is GCP-native: Artifact Registry + Cloud Run Job +
Cloud Scheduler.

> `gcr.io` (Container Registry) was shut down in March 2025 — use Artifact Registry.

## Components

```text
Cloud Scheduler (cron)
    -> Cloud Run Job "dbt-runner"
        -> scripts/check_bronze_readiness.py   (non-zero exit aborts the build)
        -> dbt seed --full-refresh
        -> dbt build
```

## Variables

```bash
export PROJECT_ID=lab-spanish-smarkts-scraper
export REGION=europe-southwest1
export REPO=containers
export IMAGE_URI="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/dbt-runner:latest"
export RUNNER_SA="dbt-runner@$PROJECT_ID.iam.gserviceaccount.com"
```

## 1. Artifact Registry repository (one-off)

```bash
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID"
```

## 2. Service account and IAM (one-off)

The container authenticates with `method: oauth`, which resolves to the job's
attached service account via ADC — no keyfile is baked into the image.

```bash
gcloud iam service-accounts create dbt-runner \
  --display-name="dbt runner (Cloud Run Job)" \
  --project="$PROJECT_ID"

# Read Bronze, write Silver/Gold, run query jobs.
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$RUNNER_SA" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$RUNNER_SA" --role="roles/bigquery.jobUser"
```

## 3. Build the image

```bash
gcloud builds submit \
  --config=cloudbuild.dbt.yaml \
  --substitutions=_IMAGE_URI="$IMAGE_URI" \
  --project="$PROJECT_ID" .
```

Build context is the repo root — the Dockerfile copies both `dbt/supermarket_dwh`
and `scripts/check_bronze_readiness.py`.

## 4. Create the Cloud Run Job

```bash
gcloud run jobs create dbt-runner \
  --image="$IMAGE_URI" \
  --region="$REGION" \
  --service-account="$RUNNER_SA" \
  --cpu=1 \
  --memory=2Gi \
  --max-retries=1 \
  --task-timeout=30m \
  --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,DBT_DATASET=dwh_silver_dev,DBT_LOCATION=$REGION,BRONZE_DATASET=dwh_bronze_dev,DBT_THREADS=4" \
  --project="$PROJECT_ID"
```

Update an existing job after a new image push:

```bash
gcloud run jobs update dbt-runner --image="$IMAGE_URI" \
  --region="$REGION" --project="$PROJECT_ID"
```

Run it once by hand before scheduling:

```bash
gcloud run jobs execute dbt-runner --region="$REGION" --project="$PROJECT_ID" --wait
```

## 5. Schedule it

Pick a time comfortably after the scrapers finish. The readiness check is the real
guard — it aborts rather than building on a partial Bronze load.

```bash
gcloud scheduler jobs create http dbt-runner-daily \
  --location="$REGION" \
  --schedule="30 6 * * *" \
  --time-zone="Europe/Madrid" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/dbt-runner:run" \
  --http-method=POST \
  --oauth-service-account-email="$RUNNER_SA" \
  --project="$PROJECT_ID"
```

The scheduler SA needs permission to trigger the job:

```bash
gcloud run jobs add-iam-policy-binding dbt-runner \
  --member="serviceAccount:$RUNNER_SA" \
  --role="roles/run.invoker" \
  --region="$REGION" --project="$PROJECT_ID"
```

## Entrypoint knobs

| Env var | Default | Effect |
|---|---|---|
| `DBT_SELECT` | *(unset)* | Unset builds the whole project. Set it only for scoped ad-hoc runs — a downstream-only selector like `silver_product_standardized+` leaves the incremental intermediates stale. |
| `DBT_FULL_REFRESH` | `false` | `true` appends `--full-refresh`. Needed whenever model logic or a seed changes, since incremental models otherwise only apply new logic to new partitions. |
| `TARGET_DATE` | *(unset)* | Pins the readiness check to a specific date instead of today. |
| `BRONZE_MIN_ROWS` | `1` | Minimum rows per supermarket for Bronze to count as ready. |
| `DBT_THREADS` | `4` | dbt concurrency. |

### Full-refresh after a logic or seed change

The models in the `scrape_date` lineage are incremental, so a changed `case_size`
regex or a new taxonomy rule only affects newly built partitions. Backfill history
with a one-off execution:

```bash
gcloud run jobs execute dbt-runner \
  --region="$REGION" --project="$PROJECT_ID" --wait \
  --update-env-vars="DBT_FULL_REFRESH=true"
```

Better still, wire this into deployment so it is not a thing to remember: if a
commit touches `dbt/supermarket_dwh/models/` or `dbt/supermarket_dwh/seeds/`, run
the execution above instead of the plain one.

## Cost

At ~30k new rows/day the nightly incremental build scans well under 1 GiB; the
tests dominate at roughly 5-10 GiB. Cloud Run Jobs (1 vCPU / 2 GiB for a few
minutes) and Cloud Scheduler both sit inside their free tiers. Expect the nightly
run to be effectively free, with occasional `--full-refresh` runs (~0.7 TiB each)
as the only meaningful BigQuery cost.

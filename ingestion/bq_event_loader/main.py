import logging
import os
import re
import uuid
from datetime import datetime

import google.auth
from flask import Flask, request
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

PROJECT_ID = os.getenv("BQ_PROJECT", bq.project)
DATASET = os.getenv("BQ_DATASET", "dwh_bronze_prod")
RAW_BUCKET = os.getenv("RAW_BUCKET")  # optional sanity check

# Downstream dbt runner. Leaving DBT_JOB_NAME unset disables the trigger, so the
# loader keeps working unchanged if this is deployed before the job exists.
DBT_JOB_NAME = os.getenv("DBT_JOB_NAME")
DBT_JOB_REGION = os.getenv("DBT_JOB_REGION", "europe-southwest1")
DBT_TRIGGER_LOCK_PREFIX = os.getenv("DBT_TRIGGER_LOCK_PREFIX", "_dbt_trigger_lock_")

# Map prefix -> table name (one table per supermarket)
TABLE_MAP = {
    "carrefour": "bronze_carrefour_products_p",
    "alcampo": "bronze_alcampo_products_p",
    "dia": "bronze_dia_products_p",
    "mercadona": "bronze_mercadona_products_p",
}

# Expected object: "<supermarket>/<YYYY-MM-DD>/<something>.parquet"
PATH_RE = re.compile(r"^(?P<source>[a-z0-9_-]+)/(?P<day>\d{4}-\d{2}-\d{2})/.+\.parquet$", re.IGNORECASE)


def _all_sources_loaded(day):
    """True once every supermarket has rows for `day`.

    This is the fan-in predicate: the four parquet uploads arrive independently,
    and only the event that happens to land last sees a complete Bronze. That
    invocation is the one allowed to start dbt.
    """
    union = "\nunion all\n".join(
        f"select '{source}' as source, count(*) as n "
        f"from `{PROJECT_ID}.{DATASET}.{table}` where date = @day"
        for source, table in TABLE_MAP.items()
    )
    job = bq.query(
        f"select source, n from ({union})",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
        ),
    )
    counts = {row.source: row.n for row in job.result()}
    missing = [s for s in TABLE_MAP if counts.get(s, 0) == 0]
    if missing:
        logging.info("Bronze incomplete for %s, still missing: %s", day, ", ".join(sorted(missing)))
        return False
    return True


def _claim_trigger(day):
    """Atomically claim the right to trigger dbt for `day`.

    Eventarc delivers at-least-once, so a duplicate of the final event would
    otherwise start a second dbt run concurrently with the first. Both use
    insert_overwrite against the same partitions, which is a corruption risk, not
    a tidiness one.

    The lock is a per-day table, because `CREATE TABLE` is the atomic primitive
    here: exactly one of N concurrent callers creates it and the rest get a
    Conflict. DML is NOT a substitute — both a transactional
    `INSERT ... WHERE NOT EXISTS` and a single-statement `MERGE` were measured
    letting multiple concurrent callers through against real BigQuery. The
    marker expires on its own so the dataset does not accumulate them.
    """
    lock_table = f"{PROJECT_ID}.{DATASET}.{DBT_TRIGGER_LOCK_PREFIX}{day:%Y%m%d}"
    try:
        bq.query(
            f"create table `{lock_table}` (claimed_at timestamp) "
            "options (expiration_timestamp = timestamp_add(current_timestamp(), interval 30 day))"
        ).result()
        return True
    except Exception:
        logging.info("dbt trigger for %s already claimed, skipping", day)
        return False


def _start_dbt_job():
    url = (
        f"https://run.googleapis.com/v2/projects/{PROJECT_ID}"
        f"/locations/{DBT_JOB_REGION}/jobs/{DBT_JOB_NAME}:run"
    )
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    response = AuthorizedSession(creds).post(url, json={}, timeout=30)
    response.raise_for_status()
    logging.info("Started dbt job %s", DBT_JOB_NAME)


def maybe_trigger_dbt(day):
    """Start the warehouse build if this event completed Bronze for `day`.

    Never raises: the parquet load has already succeeded by this point, and
    returning an error would make Eventarc redeliver the event and repeat it.
    """
    if not DBT_JOB_NAME:
        return "dbt trigger disabled"
    try:
        if not _all_sources_loaded(day):
            return "bronze incomplete"
        if not _claim_trigger(day):
            return "already triggered"
        _start_dbt_job()
        return "dbt triggered"
    except Exception:
        logging.exception("Failed to trigger dbt for %s", day)
        return "dbt trigger failed"


@app.post("/")
def handle_event():
    event = request.get_json(silent=True) or {}

    # GCS eventarc payload commonly includes these fields:
    bucket = event.get("bucket") or event.get("data", {}).get("bucket")
    name = event.get("name") or event.get("data", {}).get("name")
    generation = str(event.get("generation") or event.get("data", {}).get("generation") or "")

    if not bucket or not name:
        return ("Missing bucket/name in event", 400)

    if RAW_BUCKET and bucket != RAW_BUCKET:
        return ("Ignored: bucket mismatch", 200)

    m = PATH_RE.match(name)
    if not m:
        return ("Ignored: not a parquet path we care about", 200)

    source = m.group("source").lower()
    day_str = m.group("day")
    if source not in TABLE_MAP:
        return (f"Ignored: unknown source prefix '{source}'", 200)

    table = TABLE_MAP[source]
    day = datetime.strptime(day_str, "%Y-%m-%d").date()

    uri = f"gs://{bucket}/{name}"

    # Staging table unique per event
    suffix = uuid.uuid4().hex[:10]
    stg_table_id = f"{PROJECT_ID}.{DATASET}._stg_{table}_{day_str.replace('-', '')}_{suffix}"
    final_table_id = f"{PROJECT_ID}.{DATASET}.{table}"

    # 1) Load parquet into staging
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE")
    load_job = bq.load_table_from_uri(uri, stg_table_id, job_config=job_config)
    load_job.result()

    # 2) Overwrite that day in final (delete + insert)
    sql = f"""
    BEGIN TRANSACTION;
      DELETE FROM `{final_table_id}` WHERE date = @day;
      INSERT INTO `{final_table_id}` SELECT * FROM `{stg_table_id}`;
    COMMIT TRANSACTION;
    """
    query_job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("day", "DATE", day)]
    ))
    query_job.result()

    # 3) Drop staging
    bq.delete_table(stg_table_id, not_found_ok=True)

    # 4) If this was the last supermarket to land, start the warehouse build.
    trigger_status = maybe_trigger_dbt(day)

    return (f"OK loaded {uri} -> {final_table_id} for {day_str} ({trigger_status})", 200)

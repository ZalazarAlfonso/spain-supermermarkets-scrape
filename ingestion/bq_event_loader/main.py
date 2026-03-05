import os
import re
import uuid
from datetime import datetime

from flask import Flask, request
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

PROJECT_ID = os.getenv("BQ_PROJECT", bq.project)
DATASET = os.getenv("BQ_DATASET", "dwh_bronze_prod")
RAW_BUCKET = os.getenv("RAW_BUCKET")  # optional sanity check

# Map prefix -> table name (one table per supermarket)
TABLE_MAP = {
    "carrefour": "bronze_carrefour_products_p",
    "alcampo": "bronze_alcampo_products_p",
    "dia": "bronze_dia_products_p",
    "mercadona": "bronze_mercadona_products_p",
}

# Expected object: "<supermarket>/<YYYY-MM-DD>/<something>.parquet"
PATH_RE = re.compile(r"^(?P<source>[a-z0-9_-]+)/(?P<day>\d{4}-\d{2}-\d{2})/.+\.parquet$", re.IGNORECASE)


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

    return (f"OK loaded {uri} -> {final_table_id} for {day_str}", 200)
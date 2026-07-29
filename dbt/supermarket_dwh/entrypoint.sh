#!/usr/bin/env sh
set -eu

: "${DBT_PROJECT:=${GOOGLE_CLOUD_PROJECT:-}}"
: "${DBT_DATASET:=dwh_silver_dev}"
: "${DBT_LOCATION:=europe-southwest1}"

if [ -z "$DBT_PROJECT" ]; then
  echo "ERROR: set DBT_PROJECT or GOOGLE_CLOUD_PROJECT" >&2
  exit 2
fi

mkdir -p "$DBT_PROFILES_DIR"
cat > "$DBT_PROFILES_DIR/profiles.yml" <<EOF
supermarket_dwh:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: ${DBT_PROJECT}
      dataset: ${DBT_DATASET}
      location: ${DBT_LOCATION}
      threads: ${DBT_THREADS:-4}
      timeout_seconds: ${DBT_TIMEOUT_SECONDS:-300}
      priority: interactive
EOF

READINESS_ARGS="--project $DBT_PROJECT --dataset ${BRONZE_DATASET:-dwh_bronze_dev} --min-rows ${BRONZE_MIN_ROWS:-1}"
if [ -n "${TARGET_DATE:-}" ]; then
  READINESS_ARGS="$READINESS_ARGS --target-date $TARGET_DATE"
fi
python /app/scripts/check_bronze_readiness.py $READINESS_ARGS

cd /app/dbt/supermarket_dwh
dbt seed --full-refresh

# DBT_SELECT scopes an ad-hoc run; unset (the default) builds the whole
# project so the append-only intermediates aren't left stale behind a
# downstream-only selector.
BUILD_ARGS=""
if [ -n "${DBT_SELECT:-}" ]; then
  BUILD_ARGS="--select $DBT_SELECT"
fi
if [ "${DBT_FULL_REFRESH:-false}" = "true" ]; then
  BUILD_ARGS="$BUILD_ARGS --full-refresh"
fi

dbt build $BUILD_ARGS

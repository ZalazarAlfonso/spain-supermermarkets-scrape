FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/root/.dbt

WORKDIR /app
COPY dbt/supermarket_dwh/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dbt/supermarket_dwh /app/dbt/supermarket_dwh
COPY scripts/check_bronze_readiness.py /app/scripts/check_bronze_readiness.py
COPY dbt/supermarket_dwh/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

FROM mcr.microsoft.com/playwright/python:v1.58.0-noble

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PLAYWRIGHT_ENGINES=firefox,chromium
ENV PLAYWRIGHT_MAX_RETRIES=1
ENV PLAYWRIGHT_NAV_TIMEOUT_S=18
ENV PLAYWRIGHT_SELECTOR_TIMEOUT_MS=3000
ENV TARGETS_GCS_OBJECT_NAME=lidl/lidl_categories.json

COPY scrapers/lidl/scrape_daily/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scrapers /app/scrapers

CMD ["python", "-m", "scrapers.lidl.scrape_daily.main", "--upload-to-gcs"]


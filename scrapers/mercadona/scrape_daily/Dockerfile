FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY scrapers/mercadona/scrape_daily/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scrapers /app/scrapers

# Default entrypoint runs the daily product scraper once
CMD ["python", "-m", "scrapers.mercadona.scrape_daily.main"]

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY jobs/mercadona/scrape_daily/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY jobs /app/jobs

# Default entrypoint runs the daily product scraper once
CMD ["python", "-m", "jobs.mercadona.scrape_daily.main"]

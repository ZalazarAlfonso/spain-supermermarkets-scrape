FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY scrapers/mercadona/targets_weekly/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY scrapers /app/scrapers

# Default entrypoint runs category discovery once
CMD ["python", "-m", "scrapers.mercadona.targets_weekly.main"]

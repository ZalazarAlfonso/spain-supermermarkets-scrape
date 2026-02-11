# Playwright image includes Chromium + deps for pages that require JS rendering
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Default entrypoint runs all scrapers once
CMD ["python", "/app/main.py"]

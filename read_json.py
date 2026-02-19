import json
import os 

GCS_BUCKET = os.getenv("GCS_BUCKET", "azal-smarkets-raw-eu")
GCS_PREFIX = os.getenv("GCS_PREFIX", "")
GCS_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "lab-spanish-smarkts-scraper")

file = 'files/carrefour_categories.json'

from jobs.carrefour.common import gcs

gcs.upload_file(file, GCS_BUCKET, 'carrefour/carrefour_categories.json', 'json')

gcs.read_file(GCS_BUCKET, 'carrefour/carrefour_categories.json', 'files/carrefour_categories_gcp.json')
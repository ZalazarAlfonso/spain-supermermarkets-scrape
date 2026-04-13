from google.oauth2 import service_account
from google.cloud import bigquery
import os

key_path = '/Users/alfonsozalazaae/Documents/dbt/supermarkets/lab-spanish-smarkts-scraper-874b29c749e6.json'

print(f"Checking if key file exists: {os.path.exists(key_path)}")

try:
    client = bigquery.Client.from_service_account_json(key_path)
    datasets = list(client.list_datasets(max_results=5))
    print(f"Auth check successful. Found {len(datasets)} datasets.")
    for d in datasets:
        print(f" - {d.dataset_id}")
except Exception as e:
    print(f"Failure: {e}")

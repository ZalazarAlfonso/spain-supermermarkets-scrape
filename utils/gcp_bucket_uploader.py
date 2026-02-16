# --- GCS upload helpers ---
import io, csv, os, datetime, pytz
from google.cloud import storage
from dotenv import load_dotenv

def upload_csv_file(local_path: str, bucket_name: str, object_name: str) -> str:
    """
    Function to upload blob into gcp bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path, content_type="text/csv")
    return f"gs://{bucket_name}/{object_name}"

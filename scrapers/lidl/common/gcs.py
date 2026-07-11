from pathlib import Path

from google.cloud import storage


def upload_file(local_path: str, bucket_name: str, object_name: str, object_type: str = "text/plain") -> str:
    """Upload a local file to GCS and return its gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path, content_type=object_type)
    return f"gs://{bucket_name}/{object_name}"


def read_file_text(bucket_name: str, object_name: str, encoding: str = "utf-8") -> str:
    """Download text from a GCS object."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.download_as_text(encoding=encoding)


def validate_gcs_upload_config(local_path: str, bucket_name: str, object_name: str) -> None:
    """Validate GCS upload inputs before attempting an upload."""
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Upload enabled but `GCS_BUCKET` is not set.")

    if not object_name or not object_name.strip():
        raise ValueError("Upload enabled but generated `object_name` is empty.")

    if not Path(local_path).is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")


# jobs/al_campo/common/gcs.py
from google.cloud import storage
from pathlib import Path

def upload_file(local_path: str, bucket_name: str, object_name: str, object_type: str = "text/plain") -> str:
    """
    Function to upload blob into gcp bucket.

    Args:
        bucket_name (str): Name of the bucket where the file lives.
        object_name (str): Name of the file/object.
        local_path (str): local path of the file.
        object_type (str): type of the object, default text/plain.

    Returns:
        Confirmation of the upload (str) with the uri of the file on GCP.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path, content_type=object_type)
    return f"gs://{bucket_name}/{object_name}"

def read_file_text(bucket_name: str, object_name: str, encoding: str = "utf-8") -> str:
    """
    Download text from file on GCP, mainly for scrape categories.

    Args:
        bucket_name (str): Name of the bucket where the file lives.
        object_name (str): Name of the file/object.
        encoding (str): enconding of the text file.

    Returns:
        text of the file (str).
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.download_as_text(encoding=encoding)

def validate_gcs_upload_config(local_path: str, bucket_name: str, object_name: str) -> None:
    """
    Validate the information for gcp upload.

    Args:
        local_path (str): local path of the file.
        bucket_name (str): name of the bucket on GCP.
        object_name (str): name of the object.

    Returns:
        None -> if there is anything missing it will raise an exception.
    """
    if not bucket_name or not bucket_name.strip():
        raise ValueError("Upload enabled but `GCS_BUCKET` is not set.")

    if not object_name or not object_name.strip():
        raise ValueError("Upload enabled but generated `object_name` is empty.")

    if not Path(local_path).is_file():
        raise FileNotFoundError(f"Local CSV not found: {local_path}")

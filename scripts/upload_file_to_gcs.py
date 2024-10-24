import os
import sys
import argparse
import base64
import hashlib

from google.cloud import storage
from google.oauth2 import service_account


def calculate_md5(file_path: str) -> str:
    """
    Calculates the MD5 checksum of a file.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return base64.b64encode(hash_md5.digest()).decode()

def main():
    parser = argparse.ArgumentParser(description='Upload a file to Google Cloud Storage.')
    parser.add_argument('local_file_path', help='Path to the local file to upload.')
    parser.add_argument('gcs_path', help='GCS path in the format gs://bucket_name/path/to/blob.')
    parser.add_argument('--credentials', help='Path to the GCS credentials JSON file.')

    args = parser.parse_args()

    local_file_path = args.local_file_path
    gcs_path = args.gcs_path
    credentials_path = args.credentials

    if not os.path.exists(local_file_path):
        print(f"file {local_file_path} not found")
        # Local file does not exist; do nothing
        sys.exit(1)

    if not gcs_path.startswith('gs://'):
        print("Error: GCS path must start with gs://")
        sys.exit(1)

    # Remove 'gs://' from the path
    uri_parts = gcs_path[5:].split("/", 1)
    bucket_name = uri_parts[0]
    file_name = os.path.basename(local_file_path)
    blob_name = (
        f"{uri_parts[1]}/{file_name}"
        if len(uri_parts) > 1 and uri_parts[1]
        else file_name
    )

    # Initialize the Google Cloud Storage client
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        storage_client = storage.Client(credentials=credentials)
    else:
        storage_client = storage.Client()

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        local_md5 = calculate_md5(local_file_path)

        # Check if the blob already exists on GCS
        if blob.exists():
            blob.reload()
            gcs_md5 = blob.md5_hash
            if gcs_md5 and gcs_md5 == local_md5:
                print(f"gs://{bucket_name}/{blob_name}")
            sys.exit(0)
        
        # Upload the file to GCS
        blob.upload_from_filename(local_file_path)
        print(f"gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"Error uploading file: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

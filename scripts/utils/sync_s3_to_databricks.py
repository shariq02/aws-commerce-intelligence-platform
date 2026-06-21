"""
ACIP S3 TO DATABRICKS VOLUME SYNC
Downloads JSON events from S3 Bronze bucket to local data/s3_bronze/
then uploads to Databricks Volume /Volumes/acip/bronze/raw_files/s3_events/
Replaces the manual two-step: aws s3 sync + Databricks UI upload
Uses local machine as free intermediate -- no direct S3-to-Databricks transfer
"""

import os
import json
import time
import requests
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# AWS config
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "eu-central-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "acip-dev-bronze")
S3_PREFIX             = os.getenv("S3_PREFIX", "")

# Databricks config
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN_DBT")
VOLUME_PATH      = "/Volumes/acip/bronze/raw_files/s3_events"

PROJECT_ROOT = Path(__file__).parent.parent.parent
LOCAL_DIR    = PROJECT_ROOT / "data" / "s3_bronze"
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = LOCAL_DIR / ".sync_checkpoint.json"

CONNECT_TIMEOUT = 30
READ_TIMEOUT    = 120
CHUNK_SIZE      = 4 * 1024 * 1024

print("=" * 70)
print("ACIP S3 TO DATABRICKS VOLUME SYNC")
print("=" * 70)
print(f"S3 source:     s3://{S3_BUCKET}/{S3_PREFIX}")
print(f"Local staging: {LOCAL_DIR}")
print(f"Volume target: {DATABRICKS_HOST}{VOLUME_PATH}")


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def list_s3_files(s3_client):
    """List all files in S3 bucket with their sizes and ETags."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            files.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "etag": obj["ETag"].strip('"'),
                "last_modified": obj["LastModified"].isoformat(),
            })

    return files


def download_from_s3(s3_client, s3_key, local_path):
    """Download a file from S3 to local path."""
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    s3_client.download_file(S3_BUCKET, s3_key, str(local_path))
    return local_path.stat().st_size


# ---------------------------------------------------------------------------
# Databricks Volume helpers
# ---------------------------------------------------------------------------

def get_dbx_headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    }


def upload_to_volume(local_path, volume_file_path, max_retries=3):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_file_path}"
    for attempt in range(1, max_retries + 1):
        try:
            with open(local_path, "rb") as f:
                response = requests.put(
                    url,
                    headers=get_dbx_headers(),
                    data=f,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )
            if response.status_code in (200, 204):
                return True
            else:
                print(f"    Attempt {attempt} failed: HTTP {response.status_code} -- {response.text[:150]}")
        except Exception as e:
            print(f"    Attempt {attempt} failed: {type(e).__name__}: {str(e)[:100]}")
        if attempt < max_retries:
            time.sleep(5 * attempt)
    return False


def check_volume_file_exists(volume_file_path):
    """Check if a file already exists in Databricks Volume."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_file_path}"
    response = requests.get(
        url,
        headers=get_dbx_headers(),
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    return response.status_code == 200


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def main():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print("\nERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not found in .env")
        return

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: DATABRICKS_HOST and DATABRICKS_TOKEN not found in .env")
        return

    checkpoint = load_checkpoint()
    print(f"\nPreviously synced: {len(checkpoint)} files")

    # Step 1: List S3 files
    print("\nStep 1: Listing S3 files...")
    s3_client = get_s3_client()
    s3_files = list_s3_files(s3_client)
    print(f"Found {len(s3_files)} files in s3://{S3_BUCKET}/{S3_PREFIX}")

    if not s3_files:
        print("No files found in S3. Run the generators and Flink bronze_writer first.")
        return

    # Step 2: Identify files to sync
    print("\nStep 2: Identifying files to sync...")
    to_sync = []
    for f in s3_files:
        key = f["key"]
        if key not in checkpoint:
            to_sync.append(f)
        elif checkpoint[key].get("etag") != f["etag"]:
            print(f"  Changed: {key}")
            to_sync.append(f)
        else:
            pass  # up to date

    print(f"Files to sync: {len(to_sync)} / {len(s3_files)}")

    if not to_sync:
        print("\nAll files already synced -- nothing to do")
        return

    # Step 3: Download from S3 and upload to Databricks
    print("\nStep 3: Downloading from S3 and uploading to Databricks Volume...")

    results = {"success": 0, "failed": 0, "skipped": 0}
    total = len(to_sync)

    for i, f in enumerate(to_sync, 1):
        key = f["key"]
        size_mb = f["size"] / (1024 * 1024)

        # Build local path preserving S3 folder structure
        relative_key = key[len(S3_PREFIX):].lstrip("/")
        local_path = LOCAL_DIR / relative_key
        volume_file_path = f"{VOLUME_PATH}/{relative_key}"

        print(f"\n[{i}/{total}] {key}")
        print(f"  Size:   {size_mb:.2f} MB")
        print(f"  Local:  {local_path}")
        print(f"  Volume: {volume_file_path}")

        try:
            # Download from S3
            print(f"  Downloading from S3...", end=" ", flush=True)
            downloaded_size = download_from_s3(s3_client, key, local_path)
            print(f"OK ({downloaded_size / (1024*1024):.2f} MB)")

            # Upload to Databricks Volume
            print(f"  Uploading to Volume...", end=" ", flush=True)
            success = upload_to_volume(local_path, volume_file_path)

            if success:
                print(f"OK")
                checkpoint[key] = {
                    "etag": f["etag"],
                    "size": f["size"],
                    "last_modified": f["last_modified"],
                    "local_path": str(local_path),
                    "volume_path": volume_file_path,
                }
                save_checkpoint(checkpoint)
                results["success"] += 1
            else:
                results["failed"] += 1

        except Exception as e:
            print(f"\n  ERROR: {str(e)[:150]}")
            results["failed"] += 1

    # Summary
    print("\n" + "=" * 70)
    print("SYNC SUMMARY")
    print("=" * 70)
    print(f"Success: {results['success']}")
    print(f"Failed:  {results['failed']}")
    print(f"Total:   {total}")

    print("\n" + "=" * 70)
    if results["failed"] == 0:
        print("NEXT STEP: Run notebook 07_load_s3_events_silver.py in Databricks")
    else:
        print("Re-run this script to retry failed files")
    print("=" * 70)


if __name__ == "__main__":
    main()

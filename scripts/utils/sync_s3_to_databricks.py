"""
ACIP S3 TO DATABRICKS VOLUME SYNC
Downloads JSON events from S3 Bronze bucket to local data/s3_bronze/
then uploads to Databricks Volume /Volumes/acip/bronze/raw_files/s3_events/
Replaces the manual two-step: aws s3 sync + Databricks UI upload

Fixes applied June 2026:
  1. Flattens file structure on upload -- uploads as {domain}_events.json
     at volume root so Spark can read without _tmp_ confusion
  2. Cleans old volume files before uploading new ones
  3. Uses DATABRICKS_TOKEN_DBT for auth
  4. Retry logic on upload with backoff
"""

import os
import json
import time
import requests
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "eu-central-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "acip-dev-bronze")
S3_PREFIX             = os.getenv("S3_PREFIX", "")

DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN_DBT")
VOLUME_PATH      = "/Volumes/acip/bronze/raw_files/s3_events"

PROJECT_ROOT = Path(__file__).parent.parent.parent
LOCAL_DIR    = PROJECT_ROOT / "data" / "s3_bronze"
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

CHECKPOINT_FILE = LOCAL_DIR / ".sync_checkpoint.json"

CONNECT_TIMEOUT = 30
READ_TIMEOUT    = 120
MAX_RETRIES     = 3

DOMAINS = ["ecommerce", "pharmacy", "marketplace"]

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
    """List all JSON and _tmp_ files in S3 bucket per domain."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.split("/")[-1]
            # Skip hidden files and directory markers
            if name.startswith(".") or obj["Key"].endswith("/"):
                continue
            # Skip zero-byte files
            if obj["Size"] == 0:
                continue
            # Include both finalised .json and Flink _tmp_ files
            # _tmp_ files contain complete data -- Flink just never renamed them
            if not (name.endswith(".json") or "_tmp_" in name):
                continue
            files.append({
                "key": key,
                "size": obj["Size"],
                "etag": obj["ETag"].strip('"'),
                "last_modified": obj["LastModified"].isoformat(),
            })

    return files


def group_by_domain(s3_files):
    """
    Group S3 files by domain and merge into one per domain.
    S3 structure: domain=ecommerce/YYYY-MM-DD--HH/events-*.json
    Upload as: ecommerce_events.json (one file per domain)
    """
    domain_files = {d: [] for d in DOMAINS}
    for f in s3_files:
        for domain in DOMAINS:
            if f"domain={domain}/" in f["key"]:
                domain_files[domain].append(f)
                break
    return domain_files


def download_and_merge_domain(s3_client, domain, files, local_path):
    """
    Download all S3 files for a domain and merge into one local JSON file.
    Each line is a JSON event -- merge by concatenating all lines.
    """
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    total_lines = 0
    with open(local_path, "w", encoding="utf-8") as out_f:
        for f in files:
            tmp_path = LOCAL_DIR / f"_tmp_{domain}_{f['etag'][:8]}.json"
            s3_client.download_file(S3_BUCKET, f["key"], str(tmp_path))
            with open(tmp_path, "r", encoding="utf-8") as in_f:
                for line in in_f:
                    line = line.strip()
                    if line:
                        out_f.write(line + "\n")
                        total_lines += 1
            tmp_path.unlink()

    return total_lines


# ---------------------------------------------------------------------------
# Databricks Volume helpers
# ---------------------------------------------------------------------------

def get_dbx_headers():
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def list_volume_files():
    """List all files currently in the Databricks Volume."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{VOLUME_PATH}"
    try:
        response = requests.get(
            url,
            headers=get_dbx_headers(),
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if response.status_code == 200:
            return response.json().get("contents", [])
    except Exception:
        pass
    return []


def delete_volume_file(file_path):
    """Delete a file from Databricks Volume."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    try:
        response = requests.delete(
            url,
            headers=get_dbx_headers(),
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        return response.status_code in (200, 204)
    except Exception:
        return False


def clean_volume():
    """Delete all existing files from the volume before uploading new ones."""
    print("\nCleaning old volume files...")
    files = list_volume_files()
    deleted = 0
    for f in files:
        path = f.get("path", "")
        name = f.get("name", "")
        if not path:
            continue
        # Only delete json files and domain= directories
        if name.endswith(".json") or name.startswith("domain="):
            success = delete_volume_file(path)
            if success:
                print(f"  Deleted: {name}")
                deleted += 1
            else:
                print(f"  Failed to delete: {name}")
    print(f"Cleaned {deleted} files from volume")


def upload_to_volume(local_path, volume_file_path):
    """Upload a local file to Databricks Volume with retry."""
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{volume_file_path}"

    for attempt in range(1, MAX_RETRIES + 1):
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

        if attempt < MAX_RETRIES:
            wait = 5 * attempt
            print(f"    Retrying in {wait}s...")
            time.sleep(wait)

    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print("\nERROR: AWS credentials not found in .env")
        return

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: DATABRICKS_HOST or DATABRICKS_TOKEN_DBT not found in .env")
        return

    # Step 1: List S3 files
    print("\nStep 1: Listing S3 files...")
    s3_client = get_s3_client()
    s3_files = list_s3_files(s3_client)
    print(f"Found {len(s3_files)} finalised JSON files in s3://{S3_BUCKET}/")

    if not s3_files:
        print("No finalised files found.")
        print("NOTE: Run generators + bronze_writer and wait 30s after generators")
        print("      finish before cancelling bronze_writer (allows checkpoint to complete)")
        return

    for f in s3_files:
        print(f"  {f['key']} ({f['size']/(1024*1024):.2f} MB)")

    # Step 2: Group by domain
    domain_files = group_by_domain(s3_files)
    print("\nFiles per domain:")
    for domain, files in domain_files.items():
        print(f"  {domain}: {len(files)} files")

    domains_with_files = {d: f for d, f in domain_files.items() if f}
    if not domains_with_files:
        print("No domain files found -- check S3 folder structure")
        return

    # Step 3: Clean old volume files
    clean_volume()

    # Step 4: Download, merge per domain, upload
    print("\nStep 4: Download from S3, merge, upload to Databricks Volume...")
    results = {}

    for domain, files in domains_with_files.items():
        print(f"\n  {domain.upper()} ({len(files)} files):")

        # Merge all domain files into one local file
        local_path = LOCAL_DIR / f"{domain}_events.json"
        print(f"    Downloading and merging {len(files)} files...", end=" ", flush=True)
        try:
            total_lines = download_and_merge_domain(s3_client, domain, files, local_path)
            size_mb = local_path.stat().st_size / (1024 * 1024)
            print(f"OK ({total_lines:,} events, {size_mb:.2f} MB)")
        except Exception as e:
            print(f"FAILED: {str(e)[:100]}")
            results[domain] = False
            continue

        # Upload merged file to volume as flat {domain}_events.json
        volume_file_path = f"{VOLUME_PATH}/{domain}_events.json"
        print(f"    Uploading to volume...", end=" ", flush=True)
        success = upload_to_volume(local_path, volume_file_path)

        if success:
            print(f"OK")
            results[domain] = True
        else:
            print(f"FAILED")
            results[domain] = False

    # Summary
    print("\n" + "=" * 70)
    print("SYNC SUMMARY")
    print("=" * 70)
    successful = [d for d, ok in results.items() if ok]
    failed = [d for d, ok in results.items() if not ok]

    print(f"Successful: {len(successful)} -- {successful}")
    if failed:
        print(f"Failed:     {len(failed)} -- {failed}")

    print("\n" + "=" * 70)
    if not failed:
        print("NEXT STEP: Run notebook 07_load_s3_events_silver.py in Databricks")
    else:
        print("Re-run this script to retry failed domains")
    print("=" * 70)


if __name__ == "__main__":
    main()

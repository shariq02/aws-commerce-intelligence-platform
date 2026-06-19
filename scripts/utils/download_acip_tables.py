"""
ACIP DOWNLOAD - Pull exported CSVs from Databricks Volumes to local
Handles gold, quality, dbt_marts schemas
Pattern adapted from genomics download_changed_gold_tables.py
"""

import os
import requests
import time
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

CATALOG = "acip"
VOLUME_NAME = "acip_exports"

SCHEMAS_TO_DOWNLOAD = ["gold", "quality", "dbt_marts"]

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "acip_export"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_CHECKPOINT = DATA_DIR / ".download_checkpoint.json"

MAX_RETRIES = 5
RETRY_WAIT = 5
CHUNK_SIZE = 4 * 1024 * 1024
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def get_headers():
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def get_file_content(file_path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{file_path}"
    r = requests.get(url, headers=get_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if r.status_code == 200:
        return r.text
    return None


def list_directory(path):
    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{path}"
    r = requests.get(url, headers=get_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if r.status_code == 200:
        return r.json().get("contents", [])
    return []


def download_file(remote_path, local_path):
    local_path = Path(local_path)
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"

    for attempt in range(1, MAX_RETRIES + 1):
        resume_from = local_path.stat().st_size if local_path.exists() else 0
        headers = dict(get_headers())
        file_mode = "wb"

        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
            file_mode = "ab"
            print(f"  Resuming from {resume_from / (1024*1024):.1f} MB (attempt {attempt}/{MAX_RETRIES})")
        elif attempt > 1:
            print(f"  Retrying from scratch (attempt {attempt}/{MAX_RETRIES})")

        try:
            response = requests.get(url, headers=headers, stream=True,
                                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

            if response.status_code == 200 and resume_from > 0:
                print("  Server does not support Range -- restarting from 0")
                local_path.unlink(missing_ok=True)
                resume_from = 0
                file_mode = "wb"
            elif response.status_code not in (200, 206):
                print(f"  HTTP {response.status_code} -- cannot download")
                return False

            remaining = int(response.headers.get("content-length", 0))
            total_size = resume_from + remaining
            downloaded = resume_from

            with open(local_path, file_mode) as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct = downloaded / total_size * 100
                            print(f"\r  Progress: {pct:.1f}% ({downloaded/(1024*1024):.0f} MB / {total_size/(1024*1024):.0f} MB)",
                                  end="", flush=True)
            print()

            if total_size > 0 and downloaded < total_size:
                raise Exception(f"Incomplete: got {downloaded:,} bytes, expected {total_size:,}")

            return True

        except Exception as exc:
            print(f"\n  Attempt {attempt}/{MAX_RETRIES} failed: {type(exc).__name__}: {str(exc)[:150]}")
            if attempt < MAX_RETRIES:
                wait = RETRY_WAIT * (2 ** (attempt - 1))
                print(f"  Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                print(f"  All {MAX_RETRIES} attempts exhausted -- skipping")
                return False

    return False


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def load_checkpoint():
    if LOCAL_CHECKPOINT.exists():
        with open(LOCAL_CHECKPOINT, "r") as f:
            return json.load(f)
    return {}


def save_checkpoint(data):
    with open(LOCAL_CHECKPOINT, "w") as f:
        json.dump(data, f, indent=2)


def count_rows(local_file):
    count = 0
    with open(local_file, "r", encoding="utf-8") as f:
        for _ in f:
            count += 1
    return max(count - 1, 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("ACIP DOWNLOAD - Databricks Volumes to Local")
    print("=" * 70)
    print(f"Schemas: {', '.join(SCHEMAS_TO_DOWNLOAD)}")
    print(f"Output:  {DATA_DIR}")

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("\nERROR: DATABRICKS_HOST and DATABRICKS_TOKEN not found in .env")
        return

    checkpoint = load_checkpoint()
    print(f"\nLocal checkpoint: {len(checkpoint)} tables previously downloaded")

    all_results = {}

    for schema in SCHEMAS_TO_DOWNLOAD:
        print(f"\n{'=' * 70}")
        print(f"SCHEMA: {schema.upper()}")
        print("=" * 70)

        volume_base = f"/Volumes/{CATALOG}/{schema}/{VOLUME_NAME}"
        metadata_path = f"{volume_base}/.export_metadata.json"

        raw = get_file_content(metadata_path)
        if not raw:
            print(f"  No metadata found -- skipping {schema}")
            print(f"  Make sure 19_export_acip_tables.py ran successfully in Databricks")
            continue

        remote_metadata = json.loads(raw)
        print(f"  Remote metadata: {len(remote_metadata)} tables")

        schema_dir = DATA_DIR / schema
        schema_dir.mkdir(exist_ok=True)

        for table_name, remote_info in remote_metadata.items():
            checkpoint_key = f"{schema}.{table_name}"
            local_file = schema_dir / f"{table_name}.csv"

            print(f"\n  {table_name}:")

            # Resume partial download
            if local_file.exists() and checkpoint_key not in checkpoint:
                size_mb = local_file.stat().st_size / (1024 * 1024)
                print(f"    Partial file found ({size_mb:.0f} MB) -- resuming")
                download_needed = True
            elif checkpoint_key not in checkpoint:
                print(f"    New table -- downloading")
                download_needed = True
            else:
                local_info = checkpoint[checkpoint_key]
                if remote_info.get("rows") != local_info.get("rows"):
                    print(f"    Rows changed: {local_info.get('rows'):,} -> {remote_info.get('rows'):,}")
                    download_needed = True
                elif remote_info.get("columns") != local_info.get("columns"):
                    print(f"    Columns changed -- downloading")
                    download_needed = True
                else:
                    print(f"    Up to date -- skip")
                    download_needed = False

            if not download_needed:
                all_results[checkpoint_key] = True
                continue

            folder_path = f"{volume_base}/{table_name}/"
            files = list_directory(folder_path)
            csv_files = [f for f in files if f.get("path", "").endswith(".csv")]

            if not csv_files:
                print(f"    ERROR: No CSV found in volume folder")
                all_results[checkpoint_key] = False
                continue

            csv_file = csv_files[0]
            remote_path = csv_file["path"]
            size_mb = csv_file.get("file_size", 0) / (1024 * 1024)
            print(f"    File: {csv_file.get('name')} ({size_mb:.2f} MB)")

            if download_file(remote_path, local_file):
                row_count = count_rows(local_file)
                print(f"    Rows: {row_count:,}")
                print(f"    Status: OK")
                checkpoint[checkpoint_key] = remote_info.copy()
                save_checkpoint(checkpoint)
                all_results[checkpoint_key] = True
            else:
                print(f"    Status: FAILED")
                all_results[checkpoint_key] = False

    # Summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)

    successful = [k for k, v in all_results.items() if v]
    failed = [k for k, v in all_results.items() if not v]

    print(f"\nSuccessful: {len(successful)}")
    for k in successful:
        print(f"  OK  {k}")

    if failed:
        print(f"\nFailed: {len(failed)}")
        for k in failed:
            print(f"  FAIL {k}")

    print("\n" + "=" * 70)
    if not failed:
        print("NEXT STEP: Run scripts/load_acip_postgres.py")
    else:
        print("Re-run this script to resume failed downloads")
    print("=" * 70)


if __name__ == "__main__":
    main()

"""
ACIP DOWNLOAD - Pull exported CSVs from Databricks Volumes to local
Handles gold, quality, dbt_marts schemas
Pattern adapted from genomics download_changed_gold_tables.py

Fix applied June 2026:
  HTTP 416 (Range Not Satisfiable) handling -- when local file size >= remote
  file size, the resume range request fails. Fix: check sizes before resuming.
  If local >= remote: file is already complete, skip download entirely.
  If local > remote: local file is stale from old run, delete and restart.
  Previously: script tried to resume and got 416, reported as FAILED incorrectly.
"""

import os
import requests
import time
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN_DBT")

CATALOG = "acip"
VOLUME_NAME = "acip_exports"

SCHEMAS_TO_DOWNLOAD = ["gold", "quality", "dbt_marts_dbt_marts", "silver"]

PROJECT_ROOT = Path(__file__).parent.parent.parent
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


def get_remote_file_size(remote_path):
    """
    HEAD request to get remote file size without downloading.
    Returns size in bytes or None if unavailable.
    """
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"
    try:
        r = requests.head(
            url,
            headers=get_headers(),
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if r.status_code == 200:
            content_length = r.headers.get("content-length")
            if content_length:
                return int(content_length)
    except Exception:
        pass
    return None


def download_file(remote_path, local_path, remote_file_size=None):
    """
    Download a file from Databricks Volume to local path.
    Supports resume via Range header.
    Handles HTTP 416 by comparing local vs remote sizes before resuming.

    remote_file_size: known file size from directory listing (optional)
                      used for pre-check before attempting resume
    """
    local_path = Path(local_path)
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files{remote_path}"

    # FIX: Pre-check local vs remote size before attempting resume
    # Avoids HTTP 416 by detecting complete or stale files upfront
    if local_path.exists() and local_path.stat().st_size > 0:
        local_size = local_path.stat().st_size

        # Use known remote size or fetch via HEAD request
        known_remote_size = remote_file_size or get_remote_file_size(remote_path)

        if known_remote_size is not None:
            if local_size >= known_remote_size:
                # Local file is complete (or larger from a stale run)
                if local_size > known_remote_size:
                    print(f"  Local file ({local_size/(1024*1024):.1f} MB) larger than remote "
                          f"({known_remote_size/(1024*1024):.1f} MB) -- deleting and restarting")
                    local_path.unlink()
                else:
                    print(f"  Local file already complete ({local_size/(1024*1024):.1f} MB) -- skip download")
                    return True
            else:
                print(f"  Partial file ({local_size/(1024*1024):.1f} MB of "
                      f"{known_remote_size/(1024*1024):.1f} MB) -- resuming")

    for attempt in range(1, MAX_RETRIES + 1):
        local_size = local_path.stat().st_size if local_path.exists() else 0
        headers = dict(get_headers())
        file_mode = "wb"

        if local_size > 0:
            headers["Range"] = f"bytes={local_size}-"
            file_mode = "ab"
            if attempt == 1:
                print(f"  Resuming from {local_size / (1024*1024):.1f} MB (attempt {attempt}/{MAX_RETRIES})")
            else:
                print(f"  Resuming from {local_size / (1024*1024):.1f} MB (attempt {attempt}/{MAX_RETRIES})")
        elif attempt > 1:
            print(f"  Retrying from scratch (attempt {attempt}/{MAX_RETRIES})")

        try:
            response = requests.get(
                url, headers=headers, stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )

            # FIX: Handle HTTP 416 explicitly
            # 416 = Range Not Satisfiable = local file >= remote file
            if response.status_code == 416:
                print(f"  HTTP 416 -- local file is already complete or larger than remote")
                if local_path.exists():
                    local_size = local_path.stat().st_size
                    known_remote_size = get_remote_file_size(remote_path)
                    if known_remote_size and local_size > known_remote_size:
                        print(f"  Local ({local_size/(1024*1024):.1f} MB) > remote "
                              f"({known_remote_size/(1024*1024):.1f} MB) -- deleting and restarting")
                        local_path.unlink()
                        continue
                    else:
                        print(f"  File is complete -- treating as success")
                        return True
                continue

            if response.status_code == 200 and local_size > 0:
                # Server returned 200 instead of 206 -- does not support Range
                # Must restart from scratch
                print("  Server does not support Range -- restarting from 0")
                local_path.unlink(missing_ok=True)
                local_size = 0
                file_mode = "wb"
            elif response.status_code not in (200, 206):
                print(f"  HTTP {response.status_code} -- cannot download")
                if attempt < MAX_RETRIES:
                    wait = RETRY_WAIT * (2 ** (attempt - 1))
                    print(f"  Waiting {wait}s before retry...")
                    time.sleep(wait)
                continue

            remaining = int(response.headers.get("content-length", 0))
            total_size = local_size + remaining
            downloaded = local_size

            with open(local_path, file_mode) as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct = downloaded / total_size * 100
                            print(
                                f"\r  Progress: {pct:.1f}% "
                                f"({downloaded/(1024*1024):.0f} MB / "
                                f"{total_size/(1024*1024):.0f} MB)",
                                end="", flush=True
                            )
            print()

            if total_size > 0 and downloaded < total_size:
                raise Exception(
                    f"Incomplete: got {downloaded:,} bytes, expected {total_size:,}"
                )

            return True

        except Exception as exc:
            print(f"\n  Attempt {attempt}/{MAX_RETRIES} failed: "
                  f"{type(exc).__name__}: {str(exc)[:150]}")
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

            # Determine if download is needed
            if checkpoint_key not in checkpoint:
                if local_file.exists():
                    size_mb = local_file.stat().st_size / (1024 * 1024)
                    print(f"    Partial file found ({size_mb:.0f} MB) -- resuming")
                else:
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

            # FIX: delete stale local file when rows have changed
            # Prevents resume of old file with wrong content
            if local_file.exists() and checkpoint_key in checkpoint:
                local_info = checkpoint[checkpoint_key]
                if remote_info.get("rows") != local_info.get("rows"):
                    print(f"    Deleting stale local file before download")
                    local_file.unlink()

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
            remote_file_size = csv_file.get("file_size")
            print(f"    File: {csv_file.get('name')} ({size_mb:.2f} MB)")

            start = time.time()
            if download_file(remote_path, local_file, remote_file_size=remote_file_size):
                row_count = count_rows(local_file)
                duration = time.time() - start
                print(f"    Rows: {row_count:,}")
                print(f"    Duration: {duration:.1f}s")
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
        print("NEXT STEP: Run scripts/utils/load_acip_postgres.py")
    else:
        print("Re-run this script to resume failed downloads")
    print("=" * 70)


if __name__ == "__main__":
    main()

"""
ACIP Prefect Pipeline - Stage 2: Archive
=========================================
Responsibilities:
  1. Verify S3 Bronze files landed for all 3 domains
  2. Verify Databricks Volume has the 3 merged domain files
  3. Log Bronze watermark to pipeline log

Gate: PASS if S3 has files for all 3 domains and Volume has all 3 merged files
      FAIL if any domain is missing from S3 or Volume
"""

import os
import sys
import json
import time
import requests
import boto3
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
PREFECT_DIR  = Path(__file__).parent
PROJECT_ROOT = PREFECT_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    S3_CONFIG,
    AWS_CONFIG,
    DATABRICKS_CONFIG,
    get_logger,
)

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
S3_BRONZE_BUCKET  = S3_CONFIG["bronze_bucket"]
DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN_DBT")
VOLUME_PATH       = "/Volumes/acip/bronze/raw_files/s3_events"
DOMAINS           = ["ecommerce", "pharmacy", "marketplace"]
LOGS_DIR          = PROJECT_ROOT / "logs" / "pipeline"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="verify-s3-bronze-files", retries=2, retry_delay_seconds=15)
def verify_s3_bronze_files():
    logger = get_run_logger()
    logger.info(f"Verifying S3 Bronze files in s3://{S3_BRONZE_BUCKET}/...")

    s3 = boto3.client(
        "s3",
        region_name=AWS_CONFIG["region"],
        aws_access_key_id=AWS_CONFIG["access_key_id"],
        aws_secret_access_key=AWS_CONFIG["secret_access_key"],
    )

    domain_file_counts = {}
    domain_total_bytes = {}

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BRONZE_BUCKET)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            if size == 0:
                continue
            for domain in DOMAINS:
                if f"domain={domain}/" in key:
                    domain_file_counts[domain] = domain_file_counts.get(domain, 0) + 1
                    domain_total_bytes[domain] = domain_total_bytes.get(domain, 0) + size
                    break

    missing = []
    for domain in DOMAINS:
        count = domain_file_counts.get(domain, 0)
        size_mb = domain_total_bytes.get(domain, 0) / (1024 * 1024)
        if count == 0:
            logger.error(f"  {domain}: NO FILES FOUND")
            missing.append(domain)
        else:
            logger.info(f"  {domain}: {count} files ({size_mb:.2f} MB)")

    if missing:
        raise RuntimeError(
            f"S3 Bronze verification failed -- missing domains: {missing}"
        )

    logger.info("S3 Bronze verification PASSED -- all 3 domains present.")
    return {
        "file_counts": domain_file_counts,
        "total_bytes": domain_total_bytes,
    }


@task(name="verify-databricks-volume-files", retries=3, retry_delay_seconds=15)
def verify_databricks_volume_files():
    logger = get_run_logger()
    logger.info(f"Verifying Databricks Volume files at {VOLUME_PATH}...")

    url = f"{DATABRICKS_HOST}/api/2.0/fs/directories{VOLUME_PATH}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    response = requests.get(url, headers=headers, timeout=(30, 60))

    if response.status_code != 200:
        raise RuntimeError(
            f"Could not list Databricks Volume: HTTP {response.status_code} -- {response.text[:200]}"
        )

    contents = response.json().get("contents", [])
    volume_files = {
        item["name"]: item.get("file_size", 0)
        for item in contents
        if item["name"].endswith(".json")
    }

    missing = []
    for domain in DOMAINS:
        expected = f"{domain}_events.json"
        if expected not in volume_files:
            logger.error(f"  {expected}: NOT FOUND in Volume")
            missing.append(expected)
        else:
            size_mb = volume_files[expected] / (1024 * 1024)
            logger.info(f"  {expected}: {size_mb:.2f} MB")

    if missing:
        raise RuntimeError(
            f"Databricks Volume verification failed -- missing files: {missing}"
        )

    logger.info("Databricks Volume verification PASSED -- all 3 domain files present.")
    return volume_files


@task(name="log-bronze-watermark")
def log_bronze_watermark(s3_stats, volume_files):
    logger = get_run_logger()

    watermark = {
        "stage": "bronze_archive",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "s3_domain_file_counts": s3_stats["file_counts"],
        "s3_domain_total_mb": {
            d: round(b / (1024 * 1024), 2)
            for d, b in s3_stats["total_bytes"].items()
        },
        "volume_files": {
            name: round(size / (1024 * 1024), 2)
            for name, size in volume_files.items()
        },
        "status": "COMPLETE",
    }

    watermark_file = LOGS_DIR / "bronze_watermark.json"
    with open(watermark_file, "w") as f:
        json.dump(watermark, f, indent=2)

    logger.info(f"Bronze watermark logged to {watermark_file}")
    logger.info(f"  S3 file counts: {watermark['s3_domain_file_counts']}")
    logger.info(f"  Volume files (MB): {watermark['volume_files']}")

    return watermark


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="stage2-archive", log_prints=True)
def stage2_archive():
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE 2: ARCHIVE")
    logger.info("=" * 60)

    s3_stats      = verify_s3_bronze_files()
    volume_files  = verify_databricks_volume_files()
    log_bronze_watermark(s3_stats=s3_stats, volume_files=volume_files)

    logger.info("STAGE 2: COMPLETE")
    return True


if __name__ == "__main__":
    stage2_archive()

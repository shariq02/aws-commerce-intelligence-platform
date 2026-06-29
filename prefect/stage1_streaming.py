"""
ACIP Prefect Pipeline - Stage 1: Streaming
==========================================
Responsibilities:
  1. Pre-run cleanup: Redpanda topics, generator checkpoint, S3 Bronze
  2. Start bronze_writer via Flink (cluster start is inside bronze_writer)
  3. Run all 3 generators in parallel at rate 2000
  4. Wait 30s for final Flink checkpoint
  5. Cancel bronze_writer job
  6. Run S3 to Databricks Volume sync

Gate: PASS if all 3 generators complete and S3 sync succeeds
      FAIL if any generator fails or S3 sync fails
"""

import os
import sys
import json
import time
import subprocess
import threading
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from prefect import flow, task, get_run_logger

# ---------------------------------------------------------------------------
# Path setup -- derive everything from this file's location (prefect/)
# ---------------------------------------------------------------------------
PREFECT_DIR  = Path(__file__).parent
PROJECT_ROOT = PREFECT_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "generator"))

from config import (
    FLINK_CONFIG,
    S3_CONFIG,
    AWS_CONFIG,
    KAFKA_TOPICS,
    get_logger,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GENERATOR_RATE       = 2000
FLINK_WAIT_AFTER_GEN = 30          # seconds to wait after generators finish
GENERATOR_RUN_ID     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
CHECKPOINT_FILE      = PROJECT_ROOT / "data" / ".generator_checkpoint.json"
S3_BRONZE_BUCKET     = S3_CONFIG["bronze_bucket"]
FLINK_BIN            = Path(FLINK_CONFIG["home"]) / "bin" / "flink"
BRONZE_WRITER        = PROJECT_ROOT / "src" / "flink" / "bronze_writer.py"
GENERATOR_MAIN       = PROJECT_ROOT / "src" / "generator" / "main.py"
SCRIPTS_UTILS        = PROJECT_ROOT / "scripts" / "utils"

TOPICS = [
    KAFKA_TOPICS["ecommerce"],
    KAFKA_TOPICS["pharmacy"],
    KAFKA_TOPICS["marketplace"],
    KAFKA_TOPICS["anomalies"],
    KAFKA_TOPICS["dlq"],
]

DOMAINS = ["ecommerce", "pharmacy", "marketplace"]

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="cleanup-redpanda-topics", retries=2, retry_delay_seconds=10)
def cleanup_redpanda_topics():
    logger = get_run_logger()
    logger.info("Cleaning up Redpanda topics...")

    for topic in TOPICS:
        result = subprocess.run(
            ["rpk", "topic", "delete", topic],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            logger.info(f"  Deleted topic: {topic}")
        else:
            logger.warning(f"  Could not delete {topic}: {result.stderr.strip()} -- may not exist yet")

    time.sleep(2)

    for topic in TOPICS:
        result = subprocess.run(
            ["rpk", "topic", "create", topic],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            logger.info(f"  Created topic: {topic}")
        else:
            raise RuntimeError(f"Failed to create topic {topic}: {result.stderr.strip()}")

    logger.info("Redpanda topics cleaned and recreated.")


@task(name="cleanup-generator-checkpoint")
def cleanup_generator_checkpoint():
    logger = get_run_logger()
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info(f"Deleted generator checkpoint: {CHECKPOINT_FILE}")
    else:
        logger.info("No generator checkpoint found -- clean start")


@task(name="cleanup-s3-bronze", retries=2, retry_delay_seconds=15)
def cleanup_s3_bronze():
    logger = get_run_logger()
    logger.info(f"Clearing S3 Bronze bucket: s3://{S3_BRONZE_BUCKET}/")

    s3 = boto3.client(
        "s3",
        region_name=AWS_CONFIG["region"],
        aws_access_key_id=AWS_CONFIG["access_key_id"],
        aws_secret_access_key=AWS_CONFIG["secret_access_key"],
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BRONZE_BUCKET)

    keys_deleted = 0
    for page in pages:
        contents = page.get("Contents", [])
        if not contents:
            continue
        delete_keys = [{"Key": obj["Key"]} for obj in contents]
        s3.delete_objects(
            Bucket=S3_BRONZE_BUCKET,
            Delete={"Objects": delete_keys}
        )
        keys_deleted += len(delete_keys)

    logger.info(f"S3 Bronze cleared -- {keys_deleted} objects deleted.")


@task(name="start-bronze-writer", retries=1, retry_delay_seconds=10)
def start_bronze_writer():
    logger = get_run_logger()
    logger.info("Starting bronze_writer via Flink...")
    logger.info("(ensure_cluster_running() is called inside bronze_writer.py)")

    env = os.environ.copy()
    env["PYFLINK_PYTHON"] = "python3"

    process = subprocess.Popen(
        [str(FLINK_BIN), "run", "-py", str(BRONZE_WRITER)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(PROJECT_ROOT / "src" / "flink"),
    )

    # Wait briefly and check it started without immediate failure
    time.sleep(5)
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f"bronze_writer failed to start.\n"
            f"stdout: {stdout[:500]}\nstderr: {stderr[:500]}"
        )

    logger.info(f"bronze_writer started (PID: {process.pid})")
    return process.pid


@task(name="get-flink-job-id", retries=3, retry_delay_seconds=5)
def get_flink_job_id(bronze_writer_name="ACIP S3 Bronze Writer"):
    logger = get_run_logger()
    logger.info("Retrieving bronze_writer Flink job ID...")

    time.sleep(10)  # Allow job to register

    result = subprocess.run(
        [str(FLINK_BIN), "list", "-r"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"flink list failed: {result.stderr.strip()}")

    for line in result.stdout.splitlines():
        if bronze_writer_name in line:
            parts = line.strip().split()
            for part in parts:
                if len(part) == 32 and all(c in "0123456789abcdef" for c in part):
                    logger.info(f"Found Flink job ID: {part}")
                    return part

    raise RuntimeError(
        f"Could not find Flink job '{bronze_writer_name}' in job list.\n"
        f"Output:\n{result.stdout[:500]}"
    )


def _run_generator(domain, rate, run_id):
    """Run a single domain generator -- called in thread."""
    logger = get_logger(f"prefect.generator.{domain}")
    try:
        if domain == "ecommerce":
            from ecommerce_generator import EcommerceGenerator
            gen = EcommerceGenerator(
                data_path=str(PROJECT_ROOT / "data" / "raw" / "olist"),
                event_rate=rate,
                run_id=run_id,
            )
        elif domain == "pharmacy":
            from pharmacy_generator import PharmacyGenerator
            gen = PharmacyGenerator(
                data_path=str(PROJECT_ROOT / "data" / "raw" / "pharma"),
                event_rate=rate,
                run_id=run_id,
            )
        elif domain == "marketplace":
            from marketplace_generator import MarketplaceGenerator
            gen = MarketplaceGenerator(
                data_path=str(PROJECT_ROOT / "data" / "raw" / "olist"),
                event_rate=rate,
                run_id=run_id,
            )
        else:
            raise ValueError(f"Unknown domain: {domain}")

        logger.info(f"Starting {domain} generator at rate {rate}...")
        gen.generate()
        logger.info(f"{domain} generator completed.")
        return domain, True, None

    except Exception as e:
        logger.error(f"{domain} generator failed: {e}")
        return domain, False, str(e)


@task(name="run-generators-parallel", retries=0)
def run_generators_parallel(rate=GENERATOR_RATE, run_id=GENERATOR_RUN_ID):
    logger = get_run_logger()
    logger.info(f"Starting all 3 generators in parallel at rate {rate}...")

    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_run_generator, domain, rate, run_id): domain
            for domain in DOMAINS
        }
        for future in as_completed(futures):
            domain, success, error = future.result()
            results[domain] = success
            if success:
                logger.info(f"  {domain}: COMPLETE")
            else:
                logger.error(f"  {domain}: FAILED -- {error}")

    failed = [d for d, ok in results.items() if not ok]
    if failed:
        raise RuntimeError(f"Generators failed for domains: {failed}")

    logger.info("All 3 generators completed successfully.")
    return results


@task(name="wait-flink-checkpoint")
def wait_flink_checkpoint(wait_seconds=FLINK_WAIT_AFTER_GEN):
    logger = get_run_logger()
    logger.info(f"Waiting {wait_seconds}s for final Flink checkpoint to complete...")
    time.sleep(wait_seconds)
    logger.info("Checkpoint wait complete.")


@task(name="cancel-bronze-writer", retries=2, retry_delay_seconds=5)
def cancel_bronze_writer(job_id):
    logger = get_run_logger()
    logger.info(f"Cancelling bronze_writer Flink job: {job_id}")

    result = subprocess.run(
        [str(FLINK_BIN), "cancel", job_id],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        logger.info(f"bronze_writer cancelled successfully.")
    else:
        logger.warning(
            f"Cancel returned non-zero: {result.stderr.strip()}"
            f" -- job may have already finished."
        )


@task(name="sync-s3-to-databricks", retries=2, retry_delay_seconds=30)
def sync_s3_to_databricks():
    logger = get_run_logger()
    logger.info("Running S3 to Databricks Volume sync...")

    sync_script = SCRIPTS_UTILS / "sync_s3_to_databricks.py"
    sys.path.insert(0, str(SCRIPTS_UTILS))

    import importlib.util
    spec = importlib.util.spec_from_file_location("sync_s3_to_databricks", sync_script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    mod.main()
    logger.info("S3 sync complete.")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="stage1-streaming", log_prints=True)
def stage1_streaming():
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE 1: STREAMING")
    logger.info("=" * 60)

    # Pre-run cleanup
    cleanup_redpanda_topics()
    cleanup_generator_checkpoint()
    cleanup_s3_bronze()

    # Start Flink bronze_writer
    start_bronze_writer()
    job_id = get_flink_job_id()

    # Run all 3 generators in parallel
    run_generators_parallel(rate=GENERATOR_RATE, run_id=GENERATOR_RUN_ID)

    # Wait for final checkpoint then cancel
    wait_flink_checkpoint(wait_seconds=FLINK_WAIT_AFTER_GEN)
    cancel_bronze_writer(job_id=job_id)

    # Sync S3 to Databricks Volume
    sync_s3_to_databricks()

    logger.info("STAGE 1: COMPLETE")
    return True


if __name__ == "__main__":
    stage1_streaming()

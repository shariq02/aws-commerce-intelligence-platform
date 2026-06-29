"""
ACIP Prefect Pipeline - Stage 3: Silver
========================================
Responsibilities:
  1. Trigger Databricks Silver_Layer_Run job (ID: 723786623412314)
     -- runs notebooks 04, 05, 06, 07 in order
     -- writes both silver.events and flat Silver tables
  2. Poll until job completes or fails
  3. Verify Silver row counts via Databricks SQL

Gate: PASS if Databricks job succeeds
      FAIL if job fails or times out
"""

import os
import sys
import time
import requests
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

from config import DATABRICKS_CONFIG, get_logger

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN_DBT")
SILVER_JOB_ID         = 723786623412314
JOB_POLL_INTERVAL_SEC = 30
JOB_TIMEOUT_SEC       = 3600   # 1 hour max

# ---------------------------------------------------------------------------
# Databricks Jobs API helpers
# ---------------------------------------------------------------------------

def _get_headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


def _trigger_job(job_id):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    payload = {"job_id": job_id}
    response = requests.post(url, headers=_get_headers(), json=payload, timeout=(30, 60))
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to trigger job {job_id}: HTTP {response.status_code} -- {response.text[:300]}"
        )
    return response.json()["run_id"]


def _get_run_state(run_id):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get"
    response = requests.get(
        url, headers=_get_headers(), params={"run_id": run_id}, timeout=(30, 60)
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to get run state for run_id {run_id}: HTTP {response.status_code}"
        )
    data = response.json()
    state = data.get("state", {})
    return (
        state.get("life_cycle_state", "UNKNOWN"),
        state.get("result_state", ""),
        state.get("state_message", ""),
    )


def _poll_job(run_id, job_name, poll_interval=JOB_POLL_INTERVAL_SEC, timeout=JOB_TIMEOUT_SEC):
    logger = get_logger(f"prefect.stage3.{job_name}")
    start = time.time()

    terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    success_results = {"SUCCESS"}

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise RuntimeError(
                f"Job {job_name} (run_id={run_id}) timed out after {timeout}s"
            )

        life_cycle, result, message = _get_run_state(run_id)
        logger.info(
            f"  {job_name} | run_id={run_id} | {life_cycle} | {result} | {elapsed:.0f}s elapsed"
        )

        if life_cycle in terminal_states:
            if result in success_results:
                logger.info(f"  {job_name} SUCCEEDED.")
                return True
            else:
                raise RuntimeError(
                    f"Job {job_name} (run_id={run_id}) FAILED.\n"
                    f"  Life cycle: {life_cycle}\n"
                    f"  Result: {result}\n"
                    f"  Message: {message}"
                )

        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="trigger-silver-job", retries=1, retry_delay_seconds=30)
def trigger_silver_job():
    logger = get_run_logger()
    logger.info(f"Triggering Silver_Layer_Run (job_id={SILVER_JOB_ID})...")
    logger.info("  Notebooks: 04_ecommerce_silver -> 05_pharmacy_silver -> "
                "06_marketplace_silver -> 07_load_s3_events_silver")

    run_id = _trigger_job(SILVER_JOB_ID)
    logger.info(f"  Silver job triggered -- run_id={run_id}")
    return run_id


@task(name="poll-silver-job", retries=0)
def poll_silver_job(run_id):
    logger = get_run_logger()
    logger.info(f"Polling Silver_Layer_Run (run_id={run_id})...")
    _poll_job(run_id, job_name="Silver_Layer_Run")
    logger.info("Silver_Layer_Run COMPLETE.")
    return True


@task(name="verify-silver-output", retries=2, retry_delay_seconds=20)
def verify_silver_output():
    logger = get_run_logger()
    logger.info("Verifying Silver output via Databricks SQL...")

    # Use Databricks SQL Statement API to check silver.events row count
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    headers = _get_headers()

    queries = {
        "silver.events":                 "SELECT COUNT(*) FROM acip.silver.events",
        "silver.ecommerce_orders":        "SELECT COUNT(*) FROM acip.silver.ecommerce_orders",
        "silver.pharmacy_dispensing":     "SELECT COUNT(*) FROM acip.silver.pharmacy_dispensing",
        "silver.marketplace_dispatches":  "SELECT COUNT(*) FROM acip.silver.marketplace_dispatches",
    }

    results = {}
    warehouse_id = os.getenv(
        "DATABRICKS_WAREHOUSE_ID",
        "2dcc85ea1fa1b543"  # from dbt profiles.yml
    )

    for table, sql in queries.items():
        try:
            payload = {
                "statement": sql,
                "warehouse_id": warehouse_id,
                "wait_timeout": "30s",
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=(30, 60))
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get("result", {}).get("data_array", [[0]])
                count = int(rows[0][0]) if rows else 0
                results[table] = count
                if count > 0:
                    logger.info(f"  {table}: {count:,} rows -- PASS")
                else:
                    logger.warning(f"  {table}: 0 rows -- WARN")
            else:
                logger.warning(f"  Could not query {table}: HTTP {resp.status_code}")
                results[table] = -1
        except Exception as e:
            logger.warning(f"  Could not verify {table}: {e}")
            results[table] = -1

    # Gate: silver.events must have rows
    if results.get("silver.events", 0) <= 0:
        raise RuntimeError(
            f"Silver verification failed -- silver.events has 0 rows or could not be queried."
        )

    logger.info("Silver output verification PASSED.")
    return results


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="stage3-silver", log_prints=True)
def stage3_silver():
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE 3: SILVER")
    logger.info("=" * 60)

    run_id = trigger_silver_job()
    poll_silver_job(run_id=run_id)
    verify_silver_output()

    logger.info("STAGE 3: COMPLETE")
    return True


if __name__ == "__main__":
    stage3_silver()

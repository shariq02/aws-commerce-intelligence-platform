"""
ACIP Prefect Pipeline - Stage 4: Gold and dbt
===============================================
Responsibilities:
  1. Trigger Databricks Gold_Layer_Run job (ID: 317635473853681)
     -- runs notebooks 08-18 in order
     -- notebook 18 is the Gold quality validation gate
     -- job fails if notebook 18 raises Exception (HARD_FAIL)
  2. Poll until Gold job completes or fails
  3. Run dbt run (14 models)
  4. Run dbt test

Gate: PASS if Gold job succeeds AND dbt run PASS AND dbt test PASS
      FAIL if Gold job fails, dbt run errors, or dbt test fails
"""

import os
import sys
import time
import subprocess
import requests
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
GOLD_JOB_ID           = 317635473853681
JOB_POLL_INTERVAL_SEC = 30
JOB_TIMEOUT_SEC       = 5400   # 90 min max (Gold + validation can take time)
DBT_PROJECT_DIR       = PROJECT_ROOT / "dbt"
DBT_PROFILES_DIR      = Path.home() / ".dbt"
DBT_PROFILE           = "acip_dbt"

# ---------------------------------------------------------------------------
# Databricks Jobs API helpers (same pattern as stage3)
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
    logger = get_logger(f"prefect.stage4.{job_name}")
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
                    f"  Result:     {result}\n"
                    f"  Message:    {message}\n"
                    f"  NOTE: If notebook 18 (Gold validation) raised Exception, "
                    f"check Gold layer for HARD_FAIL issues before rerunning."
                )

        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="trigger-gold-job", retries=1, retry_delay_seconds=30)
def trigger_gold_job():
    logger = get_run_logger()
    logger.info(f"Triggering Gold_Layer_Run (job_id={GOLD_JOB_ID})...")
    logger.info("  Notebooks: 08_dim_date -> 09_dim_geography -> 10_dim_product -> "
                "11_dim_customer -> 12_dim_seller -> 13_fact_transactions -> "
                "14_fact_inventory_snapshots -> 15_fact_seller_performance -> "
                "16_agg_daily_domain_metrics -> 17_agg_customer_segments -> "
                "18_gold_quality_validation (gate)")

    run_id = _trigger_job(GOLD_JOB_ID)
    logger.info(f"  Gold job triggered -- run_id={run_id}")
    return run_id


@task(name="poll-gold-job", retries=0)
def poll_gold_job(run_id):
    logger = get_run_logger()
    logger.info(f"Polling Gold_Layer_Run (run_id={run_id})...")
    _poll_job(run_id, job_name="Gold_Layer_Run")
    logger.info("Gold_Layer_Run COMPLETE -- notebook 18 validation gate passed.")
    return True


@task(name="run-dbt", retries=1, retry_delay_seconds=60)
def run_dbt():
    logger = get_run_logger()
    logger.info("Running dbt run...")
    logger.info(f"  Project dir:  {DBT_PROJECT_DIR}")
    logger.info(f"  Profiles dir: {DBT_PROFILES_DIR}")
    logger.info(f"  Profile:      {DBT_PROFILE}")

    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", str(DBT_PROFILES_DIR),
            "--profile", DBT_PROFILE,
        ],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
    )

    logger.info("dbt run output:")
    for line in result.stdout.splitlines():
        logger.info(f"  {line}")

    if result.returncode != 0:
        logger.error("dbt run stderr:")
        for line in result.stderr.splitlines():
            logger.error(f"  {line}")
        raise RuntimeError(
            f"dbt run failed with exit code {result.returncode}.\n"
            f"Check output above for ERROR models."
        )

    # Check for ERROR in output
    if "ERROR" in result.stdout and "ERROR=0" not in result.stdout:
        raise RuntimeError(
            f"dbt run completed but reported errors.\n"
            f"Check output above."
        )

    logger.info("dbt run PASSED.")
    return True


@task(name="run-dbt-test", retries=1, retry_delay_seconds=30)
def run_dbt_test():
    logger = get_run_logger()
    logger.info("Running dbt test...")

    result = subprocess.run(
        [
            "dbt", "test",
            "--profiles-dir", str(DBT_PROFILES_DIR),
            "--profile", DBT_PROFILE,
        ],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR),
    )

    logger.info("dbt test output:")
    for line in result.stdout.splitlines():
        logger.info(f"  {line}")

    if result.returncode != 0:
        logger.error("dbt test stderr:")
        for line in result.stderr.splitlines():
            logger.error(f"  {line}")
        raise RuntimeError(
            f"dbt test failed with exit code {result.returncode}.\n"
            f"Check output above for failing tests."
        )

    logger.info("dbt test PASSED.")
    return True


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="stage4-gold-dbt", log_prints=True)
def stage4_gold_dbt():
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE 4: GOLD AND DBT")
    logger.info("=" * 60)

    run_id = trigger_gold_job()
    poll_gold_job(run_id=run_id)
    run_dbt()
    run_dbt_test()

    logger.info("STAGE 4: COMPLETE")
    return True


if __name__ == "__main__":
    stage4_gold_dbt()

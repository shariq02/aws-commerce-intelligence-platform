"""
ACIP Prefect Pipeline - Stage 5: Serving
==========================================
Responsibilities:
  1. Trigger Databricks Export_Run job (ID: 175419002212783)
     -- runs 19_export_acip_tables.py
     -- exports gold, quality, dbt_marts, silver to Volumes
  2. Poll until export job completes
  3. Run download_acip_tables.py -- pull CSVs from Volumes to local
  4. Run load_acip_postgres.py -- load CSVs into PostgreSQL
  5. Run fix_acip_postgres_types.py -- fix column types

Gate: PASS if all steps succeed and PostgreSQL has expected tables
      FAIL if any step fails
"""

import os
import sys
import time
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
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "utils"))

from config import DATABRICKS_CONFIG, get_logger

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN_DBT")
EXPORT_JOB_ID         = 175419002212783
JOB_POLL_INTERVAL_SEC = 30
JOB_TIMEOUT_SEC       = 1800   # 30 min max for export
SCRIPTS_UTILS         = PROJECT_ROOT / "scripts" / "utils"

# ---------------------------------------------------------------------------
# Databricks Jobs API helpers (same pattern as stage3/stage4)
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
    logger = get_logger(f"prefect.stage5.{job_name}")
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
                    f"  Message:    {message}"
                )

        time.sleep(poll_interval)


def _import_script(script_path):
    """Dynamically import a script as a module and return it."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        script_path.stem, script_path
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="trigger-export-job", retries=1, retry_delay_seconds=30)
def trigger_export_job():
    logger = get_run_logger()
    logger.info(f"Triggering Export_Run (job_id={EXPORT_JOB_ID})...")
    logger.info("  Notebook: 19_export_acip_tables.py")
    logger.info("  Exports: gold (10), quality (2), dbt_marts (7), silver (3) = 22 tables")

    run_id = _trigger_job(EXPORT_JOB_ID)
    logger.info(f"  Export job triggered -- run_id={run_id}")
    return run_id


@task(name="poll-export-job", retries=0)
def poll_export_job(run_id):
    logger = get_run_logger()
    logger.info(f"Polling Export_Run (run_id={run_id})...")
    _poll_job(run_id, job_name="Export_Run")
    logger.info("Export_Run COMPLETE.")
    return True


@task(name="download-tables", retries=2, retry_delay_seconds=30)
def download_tables():
    logger = get_run_logger()
    logger.info("Running download_acip_tables.py...")

    script = SCRIPTS_UTILS / "download_acip_tables.py"
    mod = _import_script(script)
    mod.main()

    logger.info("Download complete.")
    return True


@task(name="load-postgres", retries=1, retry_delay_seconds=30)
def load_postgres():
    logger = get_run_logger()
    logger.info("Running load_acip_postgres.py...")

    script = SCRIPTS_UTILS / "load_acip_postgres.py"
    mod = _import_script(script)
    mod.main()

    logger.info("PostgreSQL load complete.")
    return True


@task(name="fix-postgres-types", retries=1, retry_delay_seconds=15)
def fix_postgres_types():
    logger = get_run_logger()
    logger.info("Running fix_acip_postgres_types.py...")

    script = SCRIPTS_UTILS / "fix_acip_postgres_types.py"
    mod = _import_script(script)

    try:
        mod.main()
    except SystemExit as e:
        # fix_acip_postgres_types calls sys.exit(0) on success, sys.exit(1) on failure
        if e.code != 0:
            raise RuntimeError(
                f"fix_acip_postgres_types.py exited with code {e.code} -- type conversion failures detected."
            )

    logger.info("PostgreSQL type fix complete.")
    return True


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="stage5-serving", log_prints=True)
def stage5_serving():
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE 5: SERVING")
    logger.info("=" * 60)

    run_id = trigger_export_job()
    poll_export_job(run_id=run_id)
    download_tables()
    load_postgres()
    fix_postgres_types()

    logger.info("STAGE 5: COMPLETE")
    return True


if __name__ == "__main__":
    stage5_serving()

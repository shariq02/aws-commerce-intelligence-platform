"""
ACIP Prefect Pipeline - Master Pipeline
========================================
Orchestrates all 5 stages with a gate pattern.
Each stage must PASS before the next starts.
On any stage failure: SQS alert is published and pipeline stops.

Stage sequence:
  Stage 1: Streaming     (Flink + generators + S3 sync)
  Stage 2: Archive       (S3 + Volume verification + watermark)
  Stage 3: Silver        (Silver_Layer_Run Databricks job)
  Stage 4: Gold + dbt    (Gold_Layer_Run + dbt run + dbt test)
  Stage 5: Serving       (Export_Run + download + PostgreSQL + type fix)

Usage:
  python master_pipeline.py                    -- run all 5 stages
  python master_pipeline.py --from-stage 3    -- resume from stage 3
  python master_pipeline.py --stages 3 4 5    -- run specific stages only

SQS alert:
  Published on any stage failure.
  Queue URL from SQS_ALERT_QUEUE_URL in .env
  Falls back to log warning if SQS is unavailable.
"""

import os
import sys
import json
import argparse
import traceback
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

from config import AWS_CONFIG, get_logger

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Import stage flows
# ---------------------------------------------------------------------------
from stage1_streaming import stage1_streaming
from stage2_archive   import stage2_archive
from stage3_silver    import stage3_silver
from stage4_gold_dbt  import stage4_gold_dbt
from stage5_serving   import stage5_serving

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SQS_QUEUE_URL = os.getenv("SQS_ALERT_QUEUE_URL", "")

STAGE_MAP = {
    1: ("Stage 1: Streaming",  stage1_streaming),
    2: ("Stage 2: Archive",    stage2_archive),
    3: ("Stage 3: Silver",     stage3_silver),
    4: ("Stage 4: Gold + dbt", stage4_gold_dbt),
    5: ("Stage 5: Serving",    stage5_serving),
}

# ---------------------------------------------------------------------------
# SQS alert helper
# ---------------------------------------------------------------------------

def _send_sqs_alert(stage_name, error_message, run_id):
    logger = get_logger("prefect.master_pipeline")

    if not SQS_QUEUE_URL:
        logger.warning(
            "SQS_ALERT_QUEUE_URL not set in .env -- skipping SQS alert. "
            f"Stage failed: {stage_name}"
        )
        return

    try:
        import boto3
        sqs = boto3.client(
            "sqs",
            region_name=AWS_CONFIG["region"],
            aws_access_key_id=AWS_CONFIG["access_key_id"],
            aws_secret_access_key=AWS_CONFIG["secret_access_key"],
        )

        alert = {
            "source":      "ACIP Prefect Pipeline",
            "run_id":      run_id,
            "stage":       stage_name,
            "status":      "FAILED",
            "error":       error_message[:500],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }

        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(alert),
            MessageAttributes={
                "severity": {
                    "DataType": "String",
                    "StringValue": "ERROR",
                },
                "pipeline": {
                    "DataType": "String",
                    "StringValue": "acip-prefect",
                },
            },
        )
        logger.info(f"SQS alert sent for failed stage: {stage_name}")

    except Exception as e:
        logger.warning(f"Could not send SQS alert: {e}")


# ---------------------------------------------------------------------------
# Gate runner
# ---------------------------------------------------------------------------

def _run_stage_with_gate(stage_num, stage_name, stage_fn, run_id, logger):
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"GATE: Starting {stage_name}")
    logger.info("=" * 60)

    start = datetime.now(timezone.utc)
    try:
        stage_fn()
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"GATE: {stage_name} PASSED ({elapsed:.0f}s)")
        return True

    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        error_msg = str(e)
        tb = traceback.format_exc()

        logger.error(f"GATE: {stage_name} FAILED ({elapsed:.0f}s)")
        logger.error(f"  Error: {error_msg}")
        logger.error(f"  Traceback:\n{tb}")

        _send_sqs_alert(
            stage_name=stage_name,
            error_message=error_msg,
            run_id=run_id,
        )

        raise RuntimeError(
            f"Pipeline stopped at {stage_name}.\n"
            f"Error: {error_msg}\n"
            f"Fix the issue and re-run with --from-stage {stage_num}"
        )


# ---------------------------------------------------------------------------
# Master flow
# ---------------------------------------------------------------------------

@flow(name="acip-master-pipeline", log_prints=True)
def master_pipeline(stages_to_run=None):
    logger = get_run_logger()

    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

    if stages_to_run is None:
        stages_to_run = list(STAGE_MAP.keys())

    logger.info("=" * 60)
    logger.info("ACIP MASTER PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Run ID:         {run_id}")
    logger.info(f"Stages to run:  {stages_to_run}")
    logger.info(f"Start time:     {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    pipeline_start = datetime.now(timezone.utc)
    results = {}

    for stage_num in stages_to_run:
        if stage_num not in STAGE_MAP:
            raise ValueError(f"Invalid stage number: {stage_num}. Valid: {list(STAGE_MAP.keys())}")

        stage_name, stage_fn = STAGE_MAP[stage_num]
        _run_stage_with_gate(
            stage_num=stage_num,
            stage_name=stage_name,
            stage_fn=stage_fn,
            run_id=run_id,
            logger=logger,
        )
        results[stage_num] = "PASSED"

    # Final summary
    elapsed = (datetime.now(timezone.utc) - pipeline_start).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Run ID:       {run_id}")
    logger.info(f"Total time:   {elapsed:.0f}s ({elapsed/60:.1f} min)")
    logger.info(f"Stages run:   {len(results)}")
    for stage_num, status in results.items():
        stage_name = STAGE_MAP[stage_num][0]
        logger.info(f"  {stage_name}: {status}")
    logger.info("=" * 60)

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="ACIP Master Pipeline -- runs all 5 stages with gate pattern"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--from-stage",
        type=int,
        metavar="N",
        help="Resume pipeline from stage N (e.g. --from-stage 3 runs stages 3, 4, 5)",
    )
    group.add_argument(
        "--stages",
        type=int,
        nargs="+",
        metavar="N",
        help="Run specific stages only (e.g. --stages 3 4 5)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.from_stage:
        stages = list(range(args.from_stage, max(STAGE_MAP.keys()) + 1))
    elif args.stages:
        stages = sorted(args.stages)
    else:
        stages = list(STAGE_MAP.keys())

    print(f"Running stages: {stages}")
    master_pipeline(stages_to_run=stages)

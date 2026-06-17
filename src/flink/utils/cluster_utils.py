import subprocess
import time
import logging
import requests

logger = logging.getLogger(__name__)

FLINK_REST_URL = "http://localhost:8090"
FLINK_HOME = "/home/sharique/flink-2.2.0"


def ensure_cluster_running():
    try:
        response = requests.get(f"{FLINK_REST_URL}/jobs", timeout=3)
        if response.status_code == 200:
            logger.info("Flink cluster already running.")
            return
    except Exception:
        logger.info("Flink cluster not running. Starting cluster...")

    subprocess.run(
        [f"{FLINK_HOME}/bin/start-cluster.sh"],
        check=True,
    )
    time.sleep(5)
    logger.info("Flink cluster started successfully.")
import json
import logging
import boto3
import time
import os
import sys
import math
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"))
from cluster_utils import ensure_cluster_running

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["ecommerce.events", "pharmacy.events", "marketplace.events"]
DYNAMODB_ANOMALIES = "acip-dev-anomaly-flags"
DYNAMODB_METRICS = "acip-dev-domain-realtime-metrics"
REGION = "eu-central-1"
WINDOW_SIZE_MINUTES = 10
MAX_WINDOWS = 6


class AnomalyDetector(MapFunction):
    def __init__(self):
        self.dynamodb = None
        self.window_counts = {}
        self.current_window = {}
        self.last_window_time = {}

    def open(self, runtime_context):
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)

    def _get_window_key(self, now):
        minute = (now.minute // WINDOW_SIZE_MINUTES) * WINDOW_SIZE_MINUTES
        return now.strftime(f"%Y-%m-%dT%H:{minute:02d}")

    def _compute_stats(self, counts):
        if len(counts) < 2:
            return 0.0, 0.0
        mean = sum(counts) / len(counts)
        variance = sum((x - mean) ** 2 for x in counts) / len(counts)
        std = math.sqrt(variance)
        return mean, std

    def map(self, value):
        try:
            event = json.loads(value)
            domain = event.get("domain", "unknown")
            if domain == "unknown":
                return value

            now = datetime.now(timezone.utc)
            window_key = self._get_window_key(now)
            bucket = f"{domain}#{window_key}"

            if bucket not in self.current_window:
                self.current_window[bucket] = 0
                if domain not in self.last_window_time:
                    self.last_window_time[domain] = window_key

                if self.last_window_time.get(domain) != window_key:
                    old_bucket = f"{domain}#{self.last_window_time[domain]}"
                    old_count = self.current_window.pop(old_bucket, 0)
                    if domain not in self.window_counts:
                        self.window_counts[domain] = []
                    self.window_counts[domain].append(old_count)
                    if len(self.window_counts[domain]) > MAX_WINDOWS:
                        self.window_counts[domain].pop(0)
                    self.last_window_time[domain] = window_key

            self.current_window[bucket] = self.current_window.get(bucket, 0) + 1
            current_count = self.current_window[bucket]

            historical = self.window_counts.get(domain, [])
            if len(historical) >= 2:
                mean, std = self._compute_stats(historical)
                threshold = mean + (2 * std)
                if threshold > 0 and current_count > threshold:
                    self._write_anomaly(
                        domain, window_key, current_count,
                        mean, std, threshold, now
                    )

        except Exception as e:
            logger.error(f"Anomaly detector error: {e}")

        return value

    def _write_anomaly(self, domain, window_key, count,
                       mean, std, threshold, now):
        try:
            ttl_7d = int(time.time()) + 604800
            pk = f"{domain}#volume_spike#{window_key}"
            self.dynamodb.put_item(
                TableName=DYNAMODB_ANOMALIES,
                Item={
                    "pk": {"S": pk},
                    "domain": {"S": domain},
                    "anomaly_type": {"S": "volume_spike"},
                    "window_key": {"S": window_key},
                    "event_count": {"N": str(count)},
                    "historical_mean": {"N": str(round(mean, 2))},
                    "historical_std": {"N": str(round(std, 2))},
                    "threshold": {"N": str(round(threshold, 2))},
                    "severity": {"S": "high"},
                    "resolved": {"BOOL": False},
                    "created_at": {"S": now.isoformat()},
                    "ttl": {"N": str(ttl_7d)},
                },
            )
            logger.warning(
                f"Volume spike detected: domain={domain} "
                f"count={count} mean={mean:.1f} threshold={threshold:.1f}"
            )
        except Exception as e:
            logger.error(f"Failed to write anomaly: {e}")


def main():
    ensure_cluster_running()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    jar_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "jars"
    )
    kafka_jar = os.path.join(jar_dir, "flink-connector-kafka-5.0.0-2.2.jar")
    env.add_jars(f"file://{kafka_jar}")

    streams = []
    for topic in TOPICS:
        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(BOOTSTRAP_SERVERS)
            .set_topics(topic)
            .set_group_id(f"acip-anomaly-detector-{topic}")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"Anomaly Source - {topic}",
        )
        streams.append(stream)

    unified_stream = streams[0].union(*streams[1:])

    unified_stream.map(
        AnomalyDetector(),
        output_type=Types.STRING(),
    )

    logger.info("Starting Anomaly Detector...")
    env.execute("ACIP Anomaly Detector")


if __name__ == "__main__":
    main()

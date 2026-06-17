import json
import logging
import boto3
import time
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"))
from cluster_utils import ensure_cluster_running

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common.typeinfo import Types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["ecommerce.events", "pharmacy.events", "marketplace.events"]
DLQ_TOPIC = "platform.dlq"
DYNAMODB_DLQ = "acip-dev-platform-dlq-status"
REGION = "eu-central-1"

REQUIRED_FIELDS = [
    "event_id", "event_type", "event_version",
    "domain", "source_system", "occurred_at",
    "ingested_at", "correlation_id", "payload",
]
VALID_DOMAINS = ["ecommerce", "pharmacy", "marketplace"]


class ValidateEvent(MapFunction):
    def map(self, value):
        try:
            event = json.loads(value)
            missing = [f for f in REQUIRED_FIELDS if f not in event or event[f] is None]
            if missing:
                return (False, value, f"Missing fields: {missing}", event.get("domain", "unknown"))
            if event["domain"] not in VALID_DOMAINS:
                return (False, value, f"Invalid domain: {event['domain']}", "unknown")
            if not isinstance(event["payload"], dict):
                return (False, value, "Payload must be a dict", event.get("domain", "unknown"))
            return (True, value, "", event.get("domain", "unknown"))
        except json.JSONDecodeError as e:
            return (False, value, f"Invalid JSON: {e}", "unknown")
        except Exception as e:
            return (False, value, f"Validation error: {e}", "unknown")


class FilterInvalid(FilterFunction):
    def filter(self, value):
        is_valid, _, _, _ = value
        return not is_valid


class WriteToDLQ(MapFunction):
    def __init__(self):
        self.dynamodb = None
        self.dlq_counts = {}
        self.total_counts = {}
        self.last_flush = 0

    def open(self, runtime_context):
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)
        self.last_flush = time.time()

    def map(self, value):
        is_valid, raw_event, error_reason, domain = value
        now = datetime.now(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H:%M")

        self.dlq_counts[domain] = self.dlq_counts.get(domain, 0) + 1
        self.total_counts[domain] = self.total_counts.get(domain, 0) + 1

        logger.warning(f"DLQ event: domain={domain} reason={error_reason}")

        if time.time() - self.last_flush > 60:
            self._flush_stats(now, minute_key)
            self.last_flush = time.time()

        return raw_event

    def _flush_stats(self, now, minute_key):
        ttl_7d = int(time.time()) + 604800
        for domain, dlq_count in self.dlq_counts.items():
            total = self.total_counts.get(domain, 1)
            dlq_rate = (dlq_count / total) * 100 if total > 0 else 0.0
            try:
                pk = f"{domain}#{minute_key}"
                self.dynamodb.put_item(
                    TableName=DYNAMODB_DLQ,
                    Item={
                        "pk": {"S": pk},
                        "domain": {"S": domain},
                        "window_start": {"S": minute_key},
                        "dlq_count": {"N": str(dlq_count)},
                        "total_events": {"N": str(total)},
                        "dlq_rate_pct": {"N": str(round(dlq_rate, 2))},
                        "alert_triggered": {"BOOL": dlq_rate > 5.0},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_7d)},
                    },
                )
                if dlq_rate > 5.0:
                    logger.warning(
                        f"DLQ rate alert: domain={domain} "
                        f"rate={dlq_rate:.1f}%"
                    )
            except Exception as e:
                logger.error(f"Failed to write DLQ stats: {e}")


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
            .set_group_id(f"acip-dlq-router-{topic}")
            .set_starting_offsets(
                KafkaOffsetsInitializer.committed_offsets(
                    KafkaOffsetsInitializer.earliest()
                )
            )
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"DLQ Source - {topic}",
        )
        streams.append(stream)

    unified_stream = streams[0].union(*streams[1:])

    validated = unified_stream.map(
        ValidateEvent(),
        output_type=Types.TUPLE([
            Types.BOOLEAN(), Types.STRING(),
            Types.STRING(), Types.STRING(),
        ])
    )

    invalid_stream = validated.filter(FilterInvalid())

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    invalid_stream.map(
        WriteToDLQ(),
        output_type=Types.STRING(),
    ).sink_to(dlq_sink)

    logger.info("Starting DLQ Router...")
    env.execute("ACIP DLQ Router")


if __name__ == "__main__":
    main()

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
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ["ecommerce.events", "pharmacy.events", "marketplace.events"]
DYNAMODB_METRICS = "acip-dev-domain-realtime-metrics"
REGION = "eu-central-1"


class ParseDomainEvent(MapFunction):
    def map(self, value):
        try:
            event = json.loads(value)
            domain = event.get("domain", "unknown")
            event_type = event.get("event_type", "unknown")
            occurred_at = event.get("occurred_at", "")
            payload = event.get("payload", {})
            amount = float(
                payload.get("total_amount",
                payload.get("unit_price",
                payload.get("price", 0.0)))
            )
            return (domain, event_type, amount, occurred_at)
        except Exception as e:
            logger.error(f"Failed to parse domain event: {e}")
            return ("unknown", "unknown", 0.0, "")


class WriteHourlyMetrics(MapFunction):
    def __init__(self):
        self.dynamodb = None
        self.hourly_counts = {}
        self.hourly_revenue = {}
        self.last_flush = 0

    def open(self, runtime_context):
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)
        self.last_flush = time.time()

    def map(self, value):
        domain, event_type, amount, occurred_at = value

        if domain == "unknown":
            return value

        now = datetime.now(timezone.utc)
        hour_key = now.strftime("%Y-%m-%dT%H")
        bucket = f"{domain}#{hour_key}"

        self.hourly_counts[bucket] = self.hourly_counts.get(bucket, 0) + 1
        self.hourly_revenue[bucket] = self.hourly_revenue.get(bucket, 0.0) + amount

        if time.time() - self.last_flush > 60:
            self._flush_to_dynamodb(now)
            self.last_flush = time.time()

        return value

    def _flush_to_dynamodb(self, now):
        ttl_48h = int(time.time()) + 172800
        for bucket, count in self.hourly_counts.items():
            try:
                domain, hour_key = bucket.split("#", 1)
                revenue = self.hourly_revenue.get(bucket, 0.0)
                pk = f"cross_domain#{domain}#{hour_key}"
                self.dynamodb.put_item(
                    TableName=DYNAMODB_METRICS,
                    Item={
                        "pk": {"S": pk},
                        "domain": {"S": domain},
                        "metric_type": {"S": "hourly_volume"},
                        "hour_key": {"S": hour_key},
                        "transaction_count": {"N": str(count)},
                        "total_revenue": {"N": str(round(revenue, 2))},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_48h)},
                    },
                )
            except Exception as e:
                logger.error(f"Failed to flush metrics for {bucket}: {e}")


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
            .set_group_id(f"acip-cross-domain-{topic}")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"Kafka Source - {topic}",
        )
        streams.append(stream)

    unified_stream = streams[0].union(*streams[1:])

    unified_stream.map(
        ParseDomainEvent(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.STRING(),
        ])
    ).map(
        WriteHourlyMetrics(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.STRING(),
        ])
    )

    logger.info("Starting Cross-Domain Aggregator...")
    env.execute("ACIP Cross-Domain Aggregator")


if __name__ == "__main__":
    main()

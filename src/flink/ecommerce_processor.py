import json
import logging
import boto3
import time
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.common.typeinfo import Types
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"))
from cluster_utils import ensure_cluster_running

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ecommerce.events"
DYNAMODB_TABLE = "acip-dev-domain-realtime-metrics"
REGION = "eu-central-1"


class ParseEvent(MapFunction):
    def map(self, value):
        try:
            event = json.loads(value)
            event_type = event.get("event_type", "")
            payload = event.get("payload", {})
            occurred_at = event.get("occurred_at", "")
            correlation_id = event.get("correlation_id", "")
            region = payload.get("region", "unknown")
            total_amount = float(payload.get("total_amount", 0.0))
            fulfilment_time = int(payload.get("fulfilment_time_mins", 0))
            return (event_type, region, total_amount,
                    fulfilment_time, occurred_at, correlation_id)
        except Exception as e:
            logger.error(f"Failed to parse event: {e}")
            return ("unknown", "unknown", 0.0, 0, "", "")


class WriteToDynamoDB(MapFunction):
    def __init__(self):
        self.client = None

    def open(self, runtime_context):
        self.client = boto3.client("dynamodb", region_name=REGION)

    def map(self, value):
        event_type, region, total_amount, fulfilment_time, occurred_at, correlation_id = value
        if event_type == "unknown":
            return value
        try:
            window_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
            pk = f"ecommerce#{event_type}#{region}#{window_key}"
            self.client.put_item(
                TableName=DYNAMODB_TABLE,
                Item={
                    "pk": {"S": pk},
                    "domain": {"S": "ecommerce"},
                    "event_type": {"S": event_type},
                    "region": {"S": region},
                    "total_amount": {"N": str(total_amount)},
                    "fulfilment_time_mins": {"N": str(fulfilment_time)},
                    "occurred_at": {"S": occurred_at},
                    "correlation_id": {"S": correlation_id},
                    "updated_at": {"S": datetime.now(timezone.utc).isoformat()},
                    "ttl": {"N": str(int(time.time()) + 172800)},
                },
            )
        except Exception as e:
            logger.error(f"Failed to write to DynamoDB: {e}")
        return value


class DetectAnomaly(MapFunction):
    def __init__(self):
        self.event_counts = {}
        self.client = None

    def open(self, runtime_context):
        self.client = boto3.client("dynamodb", region_name=REGION)

    def map(self, value):
        event_type, region, total_amount, fulfilment_time, occurred_at, correlation_id = value
        if event_type != "order.placed":
            return value
        minute_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        key = f"{region}#{minute_key}"
        self.event_counts[key] = self.event_counts.get(key, 0) + 1
        count = self.event_counts[key]
        if count > 50:
            try:
                pk = f"ecommerce#volume_spike#{region}#{minute_key}"
                self.client.put_item(
                    TableName="acip-dev-anomaly-flags",
                    Item={
                        "pk": {"S": pk},
                        "domain": {"S": "ecommerce"},
                        "anomaly_type": {"S": "volume_spike"},
                        "region": {"S": region},
                        "count": {"N": str(count)},
                        "severity": {"S": "high"},
                        "resolved": {"BOOL": False},
                        "created_at": {"S": datetime.now(timezone.utc).isoformat()},
                        "ttl": {"N": str(int(time.time()) + 604800)},
                    },
                )
                logger.warning(f"Volume spike detected in {region}: {count} orders/minute")
            except Exception as e:
                logger.error(f"Failed to write anomaly flag: {e}")
        return value


def main():
    ensure_cluster_running()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    import os
    jar_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "jars"
    )
    kafka_jar = os.path.join(jar_dir, "flink-connector-kafka-5.0.0-2.2.jar")
    env.add_jars(f"file://{kafka_jar}")

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(TOPIC)
        .set_group_id("acip-ecommerce-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source - ecommerce.events",
    )

    parsed = stream.map(ParseEvent(), output_type=Types.TUPLE([
        Types.STRING(), Types.STRING(), Types.DOUBLE(),
        Types.INT(), Types.STRING(), Types.STRING(),
    ]))

    parsed.map(WriteToDynamoDB(), output_type=Types.TUPLE([
        Types.STRING(), Types.STRING(), Types.DOUBLE(),
        Types.INT(), Types.STRING(), Types.STRING(),
    ])).map(DetectAnomaly(), output_type=Types.TUPLE([
        Types.STRING(), Types.STRING(), Types.DOUBLE(),
        Types.INT(), Types.STRING(), Types.STRING(),
    ]))

    logger.info("Starting E-Commerce Stream Processor...")
    env.execute("ACIP E-Commerce Stream Processor")


if __name__ == "__main__":
    main()
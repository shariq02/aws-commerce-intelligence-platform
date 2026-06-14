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
TOPIC = "pharmacy.events"
DYNAMODB_METRICS = "acip-dev-domain-realtime-metrics"
DYNAMODB_INVENTORY = "acip-dev-inventory-alerts"
DYNAMODB_ANOMALIES = "acip-dev-anomaly-flags"
REGION = "eu-central-1"


class ParsePharmacyEvent(MapFunction):
    def map(self, value):
        try:
            event = json.loads(value)
            event_type = event.get("event_type", "")
            payload = event.get("payload", {})
            occurred_at = event.get("occurred_at", "")
            correlation_id = event.get("correlation_id", "")
            product_id = payload.get("product_id", "unknown")
            category = payload.get("category", "unknown")
            quantity = float(payload.get("quantity", 0))
            stock_level = int(payload.get("stock_level", -1))
            reorder_threshold = int(payload.get("reorder_threshold", 0))
            days_of_supply = int(payload.get("days_of_supply", 0))
            fill_time = int(payload.get("fill_time_mins", 0))
            is_prescription = bool(payload.get("is_prescription", False))
            return (
                event_type, product_id, category, quantity,
                stock_level, reorder_threshold, days_of_supply,
                fill_time, is_prescription, occurred_at, correlation_id
            )
        except Exception as e:
            logger.error(f"Failed to parse pharmacy event: {e}")
            return ("unknown", "unknown", "unknown", 0.0,
                    -1, 0, 0, 0, False, "", "")


class ProcessPharmacyEvent(MapFunction):
    def __init__(self):
        self.dynamodb = None
        self.sales_velocity = {}

    def open(self, runtime_context):
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)

    def map(self, value):
        (event_type, product_id, category, quantity,
         stock_level, reorder_threshold, days_of_supply,
         fill_time, is_prescription, occurred_at, correlation_id) = value

        if event_type == "unknown":
            return value

        now = datetime.now(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H:%M")
        ttl_48h = int(time.time()) + 172800
        ttl_7d = int(time.time()) + 604800

        try:
            if event_type in ("prescription.submitted", "prescription.filled"):
                pk = f"pharmacy#{event_type}#{category}#{minute_key}"
                self.dynamodb.put_item(
                    TableName=DYNAMODB_METRICS,
                    Item={
                        "pk": {"S": pk},
                        "domain": {"S": "pharmacy"},
                        "event_type": {"S": event_type},
                        "product_id": {"S": product_id},
                        "category": {"S": category},
                        "quantity": {"N": str(quantity)},
                        "fill_time_mins": {"N": str(fill_time)},
                        "is_prescription": {"BOOL": is_prescription},
                        "occurred_at": {"S": occurred_at},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_48h)},
                    },
                )
                velocity_key = f"{category}#{minute_key}"
                self.sales_velocity[velocity_key] = (
                    self.sales_velocity.get(velocity_key, 0) + quantity
                )

            elif event_type == "inventory.updated" and stock_level >= 0:
                pk = f"pharmacy#{product_id}"
                alert_level = "normal"
                if days_of_supply < 3:
                    alert_level = "critical"
                elif days_of_supply < 7:
                    alert_level = "high"
                elif days_of_supply < 14:
                    alert_level = "medium"

                self.dynamodb.put_item(
                    TableName=DYNAMODB_INVENTORY,
                    Item={
                        "pk": {"S": pk},
                        "domain": {"S": "pharmacy"},
                        "product_id": {"S": product_id},
                        "stock_level": {"N": str(stock_level)},
                        "reorder_threshold": {"N": str(reorder_threshold)},
                        "days_of_supply": {"N": str(days_of_supply)},
                        "alert_level": {"S": alert_level},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_48h)},
                    },
                )

                if stock_level <= reorder_threshold:
                    anomaly_pk = f"pharmacy#inventory_threshold#{product_id}#{minute_key}"
                    self.dynamodb.put_item(
                        TableName=DYNAMODB_ANOMALIES,
                        Item={
                            "pk": {"S": anomaly_pk},
                            "domain": {"S": "pharmacy"},
                            "anomaly_type": {"S": "inventory_threshold"},
                            "product_id": {"S": product_id},
                            "stock_level": {"N": str(stock_level)},
                            "reorder_threshold": {"N": str(reorder_threshold)},
                            "severity": {"S": alert_level},
                            "resolved": {"BOOL": False},
                            "created_at": {"S": now.isoformat()},
                            "ttl": {"N": str(ttl_7d)},
                        },
                    )
                    logger.warning(
                        f"Inventory threshold breach: {product_id} "
                        f"stock={stock_level} threshold={reorder_threshold}"
                    )

        except Exception as e:
            logger.error(f"Failed to write pharmacy event to DynamoDB: {e}")

        return value


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

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(TOPIC)
        .set_group_id("acip-pharmacy-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source - pharmacy.events",
    )

    stream.map(
        ParsePharmacyEvent(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.INT(), Types.INT(), Types.INT(),
            Types.INT(), Types.BOOLEAN(), Types.STRING(), Types.STRING(),
        ])
    ).map(
        ProcessPharmacyEvent(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.INT(), Types.INT(), Types.INT(),
            Types.INT(), Types.BOOLEAN(), Types.STRING(), Types.STRING(),
        ])
    )

    logger.info("Starting Pharmacy Stream Processor...")
    env.execute("ACIP Pharmacy Stream Processor")


if __name__ == "__main__":
    main()
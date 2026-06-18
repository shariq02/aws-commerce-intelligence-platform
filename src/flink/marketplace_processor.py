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
TOPIC = "marketplace.events"
DYNAMODB_METRICS = "acip-dev-domain-realtime-metrics"
DYNAMODB_SELLER_SLA = "acip-dev-seller-sla-status"
DYNAMODB_ANOMALIES = "acip-dev-anomaly-flags"
REGION = "eu-central-1"


class ParseMarketplaceEvent(MapFunction):
    def map(self, value):
        try:
            event = json.loads(value)
            event_type = event.get("event_type", "")
            payload = event.get("payload", {})
            occurred_at = event.get("occurred_at", "")
            correlation_id = event.get("correlation_id", "")
            seller_id = payload.get("seller_id", "unknown")
            seller_tier = payload.get("seller_tier", "unknown")
            listing_id = payload.get("listing_id", "unknown")
            dispatch_time = int(payload.get("dispatch_time_mins", 0))
            sla_threshold = int(payload.get("sla_threshold_mins", 120))
            is_sla_breached = bool(payload.get("is_sla_breached", False))
            old_price = float(payload.get("old_price", 0.0))
            new_price = float(payload.get("new_price", 0.0))
            change_pct = float(payload.get("change_pct", 0.0))
            category = payload.get("category", "unknown")
            return (
                event_type, seller_id, seller_tier, listing_id,
                dispatch_time, sla_threshold, is_sla_breached,
                old_price, new_price, change_pct, category,
                occurred_at, correlation_id
            )
        except Exception as e:
            logger.error(f"Failed to parse marketplace event: {e}")
            return (
                "unknown", "unknown", "unknown", "unknown",
                0, 120, False, 0.0, 0.0, 0.0, "unknown", "", ""
            )


class ProcessMarketplaceEvent(MapFunction):
    def __init__(self):
        self.dynamodb = None
        self.seller_stats = {}

    def open(self, runtime_context):
        self.dynamodb = boto3.client("dynamodb", region_name=REGION)

    def map(self, value):
        (event_type, seller_id, seller_tier, listing_id,
         dispatch_time, sla_threshold, is_sla_breached,
         old_price, new_price, change_pct, category,
         occurred_at, correlation_id) = value

        if event_type == "unknown":
            return value

        now = datetime.now(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H:%M")
        ttl_24h = int(time.time()) + 86400
        ttl_7d = int(time.time()) + 604800

        try:
            if event_type == "seller.order.dispatched":
                if seller_id not in self.seller_stats:
                    self.seller_stats[seller_id] = {
                        "total": 0, "breached": 0, "tier": seller_tier
                    }
                self.seller_stats[seller_id]["total"] += 1
                if is_sla_breached:
                    self.seller_stats[seller_id]["breached"] += 1

                stats = self.seller_stats[seller_id]
                breach_rate = (
                    stats["breached"] / stats["total"]
                    if stats["total"] > 0 else 0.0
                )
                sla_alert = breach_rate > 0.20

                self.dynamodb.put_item(
                    TableName=DYNAMODB_SELLER_SLA,
                    Item={
                        "pk": {"S": seller_id},
                        "seller_id": {"S": seller_id},
                        "seller_tier": {"S": seller_tier},
                        "breach_rate_session": {"N": str(round(breach_rate, 4))},
                        "total_dispatches_session": {"N": str(stats["total"])},
                        "sla_alert": {"BOOL": sla_alert},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_24h)},
                    },
                )

                pk_metric = f"marketplace#dispatch#{seller_id}#{minute_key}"
                self.dynamodb.put_item(
                    TableName=DYNAMODB_METRICS,
                    Item={
                        "pk": {"S": pk_metric},
                        "domain": {"S": "marketplace"},
                        "event_type": {"S": event_type},
                        "seller_id": {"S": seller_id},
                        "dispatch_time_mins": {"N": str(dispatch_time)},
                        "is_sla_breached": {"BOOL": is_sla_breached},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_24h)},
                    },
                )

                if sla_alert:
                    anomaly_pk = f"marketplace#sla_breach#{seller_id}#{minute_key}"
                    self.dynamodb.put_item(
                        TableName=DYNAMODB_ANOMALIES,
                        Item={
                            "pk": {"S": anomaly_pk},
                            "domain": {"S": "marketplace"},
                            "anomaly_type": {"S": "sla_breach_rate"},
                            "seller_id": {"S": seller_id},
                            "breach_rate": {"N": str(round(breach_rate, 4))},
                            "severity": {"S": "high"},
                            "resolved": {"BOOL": False},
                            "created_at": {"S": now.isoformat()},
                            "ttl": {"N": str(ttl_7d)},
                        },
                    )
                    logger.warning(
                        f"SLA breach rate alert: seller={seller_id} "
                        f"rate={breach_rate:.1%}"
                    )

            elif event_type == "price.updated":
                abs_change = abs(change_pct)
                if abs_change > 20.0:
                    anomaly_pk = f"marketplace#price_jump#{listing_id}#{minute_key}"
                    self.dynamodb.put_item(
                        TableName=DYNAMODB_ANOMALIES,
                        Item={
                            "pk": {"S": anomaly_pk},
                            "domain": {"S": "marketplace"},
                            "anomaly_type": {"S": "price_volatility"},
                            "seller_id": {"S": seller_id},
                            "listing_id": {"S": listing_id},
                            "old_price": {"N": str(old_price)},
                            "new_price": {"N": str(new_price)},
                            "change_pct": {"N": str(change_pct)},
                            "severity": {"S": "high" if abs_change > 50 else "medium"},
                            "resolved": {"BOOL": False},
                            "created_at": {"S": now.isoformat()},
                            "ttl": {"N": str(ttl_7d)},
                        },
                    )
                    logger.warning(
                        f"Price volatility alert: listing={listing_id} "
                        f"change={change_pct:.1f}%"
                    )

            elif event_type == "listing.created":
                pk_metric = f"marketplace#listing#{seller_id}#{minute_key}"
                self.dynamodb.put_item(
                    TableName=DYNAMODB_METRICS,
                    Item={
                        "pk": {"S": pk_metric},
                        "domain": {"S": "marketplace"},
                        "event_type": {"S": event_type},
                        "seller_id": {"S": seller_id},
                        "seller_tier": {"S": seller_tier},
                        "category": {"S": category},
                        "updated_at": {"S": now.isoformat()},
                        "ttl": {"N": str(ttl_24h)},
                    },
                )

        except Exception as e:
            logger.error(f"Failed to write marketplace event to DynamoDB: {e}")

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
        .set_group_id("acip-marketplace-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source - marketplace.events",
    )

    stream.map(
        ParseMarketplaceEvent(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.BOOLEAN(),
            Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(),
            Types.STRING(), Types.STRING(),
        ])
    ).map(
        ProcessMarketplaceEvent(),
        output_type=Types.TUPLE([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.INT(), Types.INT(), Types.BOOLEAN(),
            Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(),
            Types.STRING(), Types.STRING(),
        ])
    )

    logger.info("Starting Marketplace Stream Processor...")
    env.execute("ACIP Marketplace Stream Processor")


if __name__ == "__main__":
    main()

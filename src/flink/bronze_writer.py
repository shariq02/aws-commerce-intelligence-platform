import os
import logging
import signal
import boto3
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.common.serialization import Encoder
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"))
from cluster_utils import ensure_cluster_running

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS  = "localhost:9092"
S3_BUCKET          = "s3://acip-dev-bronze"
S3_BUCKET_NAME     = "acip-dev-bronze"
TOPICS             = ["ecommerce.events", "pharmacy.events", "marketplace.events"]
REGION             = "eu-central-1"
DOMAINS            = ["ecommerce", "pharmacy", "marketplace"]

# Fix applied June 2026:
# Checkpoint interval reduced from 60s to 10s so files finalise quickly.
# Files stay as _tmp_ until a checkpoint completes -- with 60s interval,
# cancelling before 60s left all files as temp. 10s ensures at least one
# checkpoint completes within a short generator run.
CHECKPOINT_INTERVAL_MS = 10_000   # 10 seconds

# Rolling policy: roll file after 5 minutes OR 1 minute of inactivity
# With 10s checkpoints, completed files are committed every ~10s
ROLLING_PART_SIZE_BYTES  = 128 * 1024 * 1024  # 128 MB
ROLLING_INTERVAL_MS      = 5 * 60 * 1000       # 5 minutes
ROLLING_INACTIVITY_MS    = 60 * 1000            # 1 minute


def create_kafka_source(topic):
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(f"acip-bronze-writer-{topic}")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def get_s3_path(topic):
    domain = topic.split(".")[0]
    return f"{S3_BUCKET}/domain={domain}/"


def cleanup_todays_s3_partitions():
    """
    Delete today's S3 partitions before writing new data.
    Ensures clean rerun on same day without partial file accumulation.
    See ADR-017 Section 17.4.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = boto3.client("s3", region_name=REGION)

    for domain in DOMAINS:
        prefix = f"domain={domain}/{today}"
        try:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=prefix
            )
            if "Contents" not in response:
                logger.info(f"No existing S3 files for {prefix} - clean start")
                continue
            keys = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3.delete_objects(
                Bucket=S3_BUCKET_NAME,
                Delete={"Objects": keys}
            )
            logger.info(f"Cleared {len(keys)} S3 files for partition: {prefix}")
        except Exception as e:
            logger.warning(
                f"Could not clear S3 partition {prefix}: {e}. Proceeding anyway."
            )


def main():
    ensure_cluster_running()
    cleanup_todays_s3_partitions()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Fix: 10s checkpoint interval so files finalise within one generator run
    # Previously 60s -- files stayed _tmp_ if job cancelled before checkpoint
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    jar_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "jars"
    )
    kafka_jar = os.path.join(jar_dir, "flink-connector-kafka-5.0.0-2.2.jar")
    s3_jar    = os.path.join(jar_dir, "flink-s3-fs-hadoop-2.2.0.jar")
    env.add_jars(f"file://{kafka_jar}", f"file://{s3_jar}")

    streams = []
    for topic in TOPICS:
        source = create_kafka_source(topic)
        stream = env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"Kafka Source - {topic}",
        )
        streams.append((topic, stream))

    for topic, stream in streams:
        s3_path = get_s3_path(topic)
        sink = (
            FileSink.for_row_format(
                s3_path,
                Encoder.simple_string_encoder(),
            )
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("events")
                .with_part_suffix(".json")
                .build()
            )
            .with_rolling_policy(
                RollingPolicy.default_rolling_policy(
                    part_size=ROLLING_PART_SIZE_BYTES,
                    rollover_interval=ROLLING_INTERVAL_MS,
                    inactivity_interval=ROLLING_INACTIVITY_MS,
                )
            )
            .build()
        )
        stream.sink_to(sink).name(f"S3 Bronze Sink - {topic}")

    logger.info("Starting S3 Bronze Writer job...")
    logger.info(f"Checkpoint interval: {CHECKPOINT_INTERVAL_MS}ms")
    logger.info("NOTE: Wait at least 30 seconds after generators finish")
    logger.info("      before cancelling -- allows final checkpoint to complete")
    env.execute("ACIP S3 Bronze Writer")


if __name__ == "__main__":
    main()

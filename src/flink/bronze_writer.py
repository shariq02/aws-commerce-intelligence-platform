import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
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

BOOTSTRAP_SERVERS = "localhost:9092"
S3_BUCKET = "s3://acip-dev-bronze"
TOPICS = ["ecommerce.events", "pharmacy.events", "marketplace.events"]


def create_kafka_source(topic):
    from pyflink.common.serialization import SimpleStringSchema
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


def main():
    ensure_cluster_running()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    jar_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "jars"
    )
    kafka_jar = os.path.join(
        jar_dir, "flink-connector-kafka-5.0.0-2.2.jar"
    )
    s3_jar = os.path.join(
        jar_dir, "flink-s3-fs-hadoop-2.2.0.jar"
    )
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
                    part_size=1024 * 1024 * 128,
                    rollover_interval=5 * 60 * 1000,
                    inactivity_interval=60 * 1000,
                )
            )
            .build()
        )
        stream.sink_to(sink).name(f"S3 Bronze Sink - {topic}")

    logger.info("Starting S3 Bronze Writer job...")
    env.execute("ACIP S3 Bronze Writer")


if __name__ == "__main__":
    main()
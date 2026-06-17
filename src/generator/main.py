import argparse
import logging
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ecommerce_generator import EcommerceGenerator
from pharmacy_generator import PharmacyGenerator
from marketplace_generator import MarketplaceGenerator
from anomaly_injector import AnomalyInjector
from dlq_handler import DLQHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
OLIST_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "olist")
PHARMA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "pharma")


def run_ecommerce(event_rate, inject_anomalies, run_id):
    generator = EcommerceGenerator(
        data_path=OLIST_PATH,
        event_rate=event_rate,
        run_id=run_id,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_pharmacy(event_rate, inject_anomalies, run_id):
    generator = PharmacyGenerator(
        data_path=PHARMA_PATH,
        event_rate=event_rate,
        run_id=run_id,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_marketplace(event_rate, inject_anomalies, run_id):
    generator = MarketplaceGenerator(
        data_path=OLIST_PATH,
        event_rate=event_rate,
        run_id=run_id,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_all(event_rate, inject_anomalies, run_id):
    logger.info("Running all three domain generators sequentially...")
    run_ecommerce(event_rate, inject_anomalies, run_id)
    run_pharmacy(event_rate, inject_anomalies, run_id)
    run_marketplace(event_rate, inject_anomalies, run_id)


def main():
    parser = argparse.ArgumentParser(
        description="ACIP Data Generator - replays datasets as Kafka events"
    )
    parser.add_argument(
        "--domain",
        choices=["ecommerce", "pharmacy", "marketplace", "all"],
        default="all",
        help="Domain to generate events for (default: all)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=10,
        help="Events per second (default: 10)",
    )
    parser.add_argument(
        "--anomalies",
        action="store_true",
        help="Enable anomaly injection mode",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Pipeline run ID for checkpoint tracking (default: today's date). "
             "Use same run-id to resume an interrupted run. "
             "Use a new run-id to start fresh.",
    )
    args = parser.parse_args()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(
        f"Starting ACIP generator - domain={args.domain} "
        f"rate={args.rate} anomalies={args.anomalies} run_id={run_id}"
    )

    if args.domain == "ecommerce":
        run_ecommerce(args.rate, args.anomalies, run_id)
    elif args.domain == "pharmacy":
        run_pharmacy(args.rate, args.anomalies, run_id)
    elif args.domain == "marketplace":
        run_marketplace(args.rate, args.anomalies, run_id)
    elif args.domain == "all":
        run_all(args.rate, args.anomalies, run_id)


if __name__ == "__main__":
    main()

import argparse
import logging
import sys
import os

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


def run_ecommerce(event_rate, inject_anomalies):
    generator = EcommerceGenerator(
        data_path=OLIST_PATH,
        event_rate=event_rate,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_pharmacy(event_rate, inject_anomalies):
    generator = PharmacyGenerator(
        data_path=PHARMA_PATH,
        event_rate=event_rate,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_marketplace(event_rate, inject_anomalies):
    generator = MarketplaceGenerator(
        data_path=OLIST_PATH,
        event_rate=event_rate,
    )
    if inject_anomalies:
        injector = AnomalyInjector(generator, anomaly_probability=0.05)
        original_publish = generator.publish

        def publish_with_anomaly(topic, event, key=None):
            original_publish(topic, event, key)
            injector.maybe_inject(topic, key)

        generator.publish = publish_with_anomaly
    generator.generate()


def run_all(event_rate, inject_anomalies):
    logger.info("Running all three domain generators sequentially...")
    run_ecommerce(event_rate, inject_anomalies)
    run_pharmacy(event_rate, inject_anomalies)
    run_marketplace(event_rate, inject_anomalies)


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
    args = parser.parse_args()

    logger.info(
        f"Starting ACIP generator - domain={args.domain} "
        f"rate={args.rate} anomalies={args.anomalies}"
    )

    if args.domain == "ecommerce":
        run_ecommerce(args.rate, args.anomalies)
    elif args.domain == "pharmacy":
        run_pharmacy(args.rate, args.anomalies)
    elif args.domain == "marketplace":
        run_marketplace(args.rate, args.anomalies)
    elif args.domain == "all":
        run_all(args.rate, args.anomalies)


if __name__ == "__main__":
    main()
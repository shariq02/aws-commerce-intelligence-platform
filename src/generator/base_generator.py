import uuid
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseGenerator(ABC):

    BOOTSTRAP_SERVERS = "localhost:9092"

    def __init__(self, domain, source_system, event_rate=10):
        self.domain = domain
        self.source_system = source_system
        self.event_rate = event_rate
        self.producer = self._create_producer()

    def _create_producer(self):
        config = {
            "bootstrap.servers": self.BOOTSTRAP_SERVERS,
            "client.id": f"acip-{self.domain}-generator",
        }
        return Producer(config)

    def build_envelope(self, event_type, payload, correlation_id=None):
        now = datetime.now(timezone.utc).isoformat()
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_version": "1.0",
            "domain": self.domain,
            "source_system": self.source_system,
            "occurred_at": now,
            "ingested_at": now,
            "correlation_id": correlation_id or str(uuid.uuid4()),
            "payload": payload,
        }

    def publish(self, topic, event, key=None):
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=json.dumps(event).encode("utf-8"),
                callback=self._delivery_callback,
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish event to {topic}: {e}")

    def _delivery_callback(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} partition {msg.partition()}")

    def flush(self):
        self.producer.flush()

    @abstractmethod
    def generate(self):
        pass
import uuid
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
CHECKPOINT_FILE = os.path.join(PROJECT_ROOT, "data", ".generator_checkpoint.json")


class BaseGenerator(ABC):

    BOOTSTRAP_SERVERS = "localhost:9092"
    CHECKPOINT_INTERVAL = 1000

    def __init__(self, domain, source_system, event_rate=10, run_id=None):
        self.domain = domain
        self.source_system = source_system
        self.event_rate = event_rate
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y-%m-%d")
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

    # ------------------------------------------------------------------
    # Checkpoint methods - idempotent resume support (ADR-017)
    # ------------------------------------------------------------------

    def load_checkpoint(self):
        if not os.path.exists(CHECKPOINT_FILE):
            return 0, False
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            run_data = data.get(self.run_id, {})
            domain_data = run_data.get(self.domain, {})
            last_row = domain_data.get("last_row", 0)
            completed = domain_data.get("completed", False)
            if completed:
                logger.info(
                    f"Checkpoint: {self.domain} already completed "
                    f"for run_id={self.run_id}. Use a new --run-id to rerun."
                )
            elif last_row > 0:
                logger.info(
                    f"Checkpoint: resuming {self.domain} from row {last_row} "
                    f"for run_id={self.run_id}"
                )
            return last_row, completed
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}. Starting from row 0.")
            return 0, False

    def save_checkpoint(self, last_row, completed=False):
        try:
            os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
            data = {}
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, "r") as f:
                    data = json.load(f)
            if self.run_id not in data:
                data[self.run_id] = {}
            data[self.run_id][self.domain] = {
                "last_row": last_row,
                "completed": completed,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            with open(CHECKPOINT_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")

    def clear_checkpoint(self):
        if not os.path.exists(CHECKPOINT_FILE):
            return
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
            if self.run_id in data and self.domain in data[self.run_id]:
                del data[self.run_id][self.domain]
                with open(CHECKPOINT_FILE, "w") as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Cleared checkpoint for {self.domain} run_id={self.run_id}")
        except Exception as e:
            logger.warning(f"Could not clear checkpoint: {e}")

    @abstractmethod
    def generate(self):
        pass

import random
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class AnomalyInjector:

    def __init__(self, generator, anomaly_probability=0.05):
        self.generator = generator
        self.anomaly_probability = anomaly_probability

    def _should_inject(self):
        return random.random() < self.anomaly_probability

    def _inject_volume_spike(self, topic, key):
        logger.info(f"Injecting volume spike on {topic}")
        for _ in range(10):
            payload = {
                "order_id": f"SPIKE-{uuid.uuid4()}",
                "customer_id": f"CUST-SPIKE-{uuid.uuid4()}",
                "customer_segment": "standard",
                "region": "DE-Hamburg",
                "items": [],
                "total_amount": random.uniform(10.0, 500.0),
                "payment_method": "card",
                "channel": "web",
            }
            event = self.generator.build_envelope(
                event_type="order.placed",
                payload=payload,
                correlation_id=str(uuid.uuid4()),
            )
            self.generator.publish(topic, event, key=key)

    def _inject_price_jump(self, topic, key):
        logger.info(f"Injecting price jump on {topic}")
        payload = {
            "listing_id": f"LST-ANOMALY-{uuid.uuid4()}",
            "seller_id": f"SELLER-ANOMALY",
            "old_price": 10.0,
            "new_price": 999.0,
            "change_pct": 9890.0,
            "reason": "anomaly",
        }
        event = self.generator.build_envelope(
            event_type="price.updated",
            payload=payload,
            correlation_id=str(uuid.uuid4()),
        )
        self.generator.publish(topic, event, key=key)

    def _inject_inventory_drop(self, topic, key):
        logger.info(f"Injecting inventory drop on {topic}")
        payload = {
            "product_id": f"PROD-ANOMALY-{uuid.uuid4()}",
            "warehouse_id": "WH-Hamburg-01",
            "stock_level": 0,
            "reorder_threshold": 50,
            "days_of_supply": 0,
            "update_reason": "anomaly",
        }
        event = self.generator.build_envelope(
            event_type="inventory.updated",
            payload=payload,
            correlation_id=str(uuid.uuid4()),
        )
        self.generator.publish(topic, event, key=key)

    def maybe_inject(self, topic, key=None):
        if not self._should_inject():
            return
        anomaly_type = random.choice(
            ["volume_spike", "price_jump", "inventory_drop"]
        )
        if anomaly_type == "volume_spike":
            self._inject_volume_spike(topic, key or "anomaly")
        elif anomaly_type == "price_jump":
            self._inject_price_jump(topic, key or "anomaly")
        elif anomaly_type == "inventory_drop":
            self._inject_inventory_drop(topic, key or "anomaly")
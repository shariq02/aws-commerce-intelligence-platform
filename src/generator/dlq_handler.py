import json
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DLQ_TOPIC = "platform.dlq"


class DLQHandler:

    REQUIRED_ENVELOPE_FIELDS = [
        "event_id",
        "event_type",
        "event_version",
        "domain",
        "source_system",
        "occurred_at",
        "ingested_at",
        "correlation_id",
        "payload",
    ]

    def __init__(self, producer):
        self.producer = producer

    def validate(self, event):
        missing_fields = [
            field
            for field in self.REQUIRED_ENVELOPE_FIELDS
            if field not in event or event[field] is None
        ]
        if missing_fields:
            return False, f"Missing required fields: {missing_fields}"
        if not isinstance(event["payload"], dict):
            return False, "Payload must be a dictionary"
        if event["domain"] not in ["ecommerce", "pharmacy", "marketplace"]:
            return False, f"Invalid domain: {event['domain']}"
        return True, None

    def route_to_dlq(self, event, error_reason):
        dlq_event = {
            "original_event_id": event.get("event_id", str(uuid.uuid4())),
            "original_event_type": event.get("event_type", "unknown"),
            "original_domain": event.get("domain", "unknown"),
            "error_reason": error_reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "original_event": event,
        }
        try:
            self.producer.produce(
                topic=DLQ_TOPIC,
                key=dlq_event["original_event_id"].encode("utf-8"),
                value=json.dumps(dlq_event).encode("utf-8"),
            )
            self.producer.poll(0)
            logger.warning(
                f"Routed event {dlq_event['original_event_id']} "
                f"to DLQ. Reason: {error_reason}"
            )
        except Exception as e:
            logger.error(f"Failed to route event to DLQ: {e}")

    def validate_and_route(self, event):
        is_valid, error_reason = self.validate(event)
        if not is_valid:
            self.route_to_dlq(event, error_reason)
            return False
        return True

    def inject_malformed_event(self, generator, topic):
        malformed_event = {
            "event_type": "order.placed",
            "domain": "ecommerce",
        }
        is_valid, error_reason = self.validate(malformed_event)
        if not is_valid:
            self.route_to_dlq(malformed_event, error_reason)
            logger.info("Injected malformed event to DLQ for testing.")
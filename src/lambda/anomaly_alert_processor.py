import json
import logging
import boto3
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SNS_ECOMMERCE_ARN = os.environ.get("SNS_ECOMMERCE_ARN", "")
SNS_PHARMACY_ARN = os.environ.get("SNS_PHARMACY_ARN", "")
SNS_MARKETPLACE_ARN = os.environ.get("SNS_MARKETPLACE_ARN", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

sns_client = boto3.client("sns", region_name="eu-central-1")
dynamodb = boto3.resource("dynamodb", region_name="eu-central-1")
dedup_table = dynamodb.Table("acip-dev-anomaly-flags")

DOMAIN_SNS_MAP = {
    "ecommerce": SNS_ECOMMERCE_ARN,
    "pharmacy": SNS_PHARMACY_ARN,
    "marketplace": SNS_MARKETPLACE_ARN,
}

ALERT_COOLDOWN_MINUTES = 60


def get_alert_dedup_key(domain, anomaly_type):
    now = datetime.now(timezone.utc)
    hour_key = now.strftime("%Y-%m-%dT%H")
    return f"alert_sent#{domain}#{anomaly_type}#{hour_key}"


def is_alert_already_sent(domain, anomaly_type):
    dedup_key = get_alert_dedup_key(domain, anomaly_type)
    try:
        response = dedup_table.get_item(Key={"pk": dedup_key})
        return "Item" in response
    except Exception as e:
        logger.error(f"Dedup check failed: {e}")
        return False


def mark_alert_sent(domain, anomaly_type):
    dedup_key = get_alert_dedup_key(domain, anomaly_type)
    now = datetime.now(timezone.utc)
    ttl = int(now.timestamp()) + (ALERT_COOLDOWN_MINUTES * 60)
    try:
        dedup_table.put_item(Item={
            "pk": dedup_key,
            "domain": domain,
            "anomaly_type": anomaly_type,
            "alert_sent_at": now.isoformat(),
            "ttl": ttl,
        })
    except Exception as e:
        logger.error(f"Failed to mark alert sent: {e}")


def get_severity_label(severity):
    mapping = {
        "critical": "CRITICAL",
        "high": "HIGH",
        "medium": "MEDIUM",
        "low": "LOW"
    }
    return mapping.get(severity.lower(), "UNKNOWN")


def build_alert_message(record):
    domain = record.get("domain", {}).get("S", "unknown")
    anomaly_type = record.get("anomaly_type", {}).get("S", "unknown")
    severity = record.get("severity", {}).get("S", "unknown")
    created_at = record.get("created_at", {}).get("S", "unknown")
    pk = record.get("pk", {}).get("S", "unknown")

    lines = [
        f"AWS Commerce Intelligence Platform - Anomaly Alert",
        f"Environment: {ENVIRONMENT}",
        f"",
        f"Severity:     {get_severity_label(severity)}",
        f"Domain:       {domain}",
        f"Anomaly Type: {anomaly_type}",
        f"Detected At:  {created_at}",
        f"Record ID:    {pk}",
        f"",
    ]

    if anomaly_type == "volume_spike":
        count = record.get("event_count", {}).get("N", "N/A")
        mean = record.get("historical_mean", {}).get("N", "N/A")
        threshold = record.get("threshold", {}).get("N", "N/A")
        lines.append(f"Event Count:   {count}")
        lines.append(f"Expected Mean: {mean}")
        lines.append(f"Threshold:     {threshold}")

    elif anomaly_type == "sla_breach_rate":
        seller_id = record.get("seller_id", {}).get("S", "N/A")
        breach_rate = record.get("breach_rate", {}).get("N", "N/A")
        lines.append(f"Seller ID:    {seller_id}")
        lines.append(f"Breach Rate:  {breach_rate}")

    elif anomaly_type == "price_volatility":
        listing_id = record.get("listing_id", {}).get("S", "N/A")
        change_pct = record.get("change_pct", {}).get("N", "N/A")
        lines.append(f"Listing ID:   {listing_id}")
        lines.append(f"Price Change: {change_pct}%")

    elif anomaly_type == "inventory_threshold":
        product_id = record.get("product_id", {}).get("S", "N/A")
        stock = record.get("stock_level", {}).get("N", "N/A")
        threshold = record.get("reorder_threshold", {}).get("N", "N/A")
        lines.append(f"Product ID:   {product_id}")
        lines.append(f"Stock Level:  {stock}")
        lines.append(f"Reorder At:   {threshold}")

    lines.append("")
    lines.append(f"Note: Alerts are deduplicated - max 1 per domain per anomaly type per hour.")
    lines.append("This is an automated alert from ACIP pipeline.")
    return "\n".join(lines)


def lambda_handler(event, context):
    logger.info(f"Processing {len(event.get('Records', []))} DynamoDB stream records")

    processed = 0
    skipped_dedup = 0
    skipped_severity = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            if record.get("eventName") not in ("INSERT", "MODIFY"):
                continue

            new_image = record.get("dynamodb", {}).get("NewImage", {})
            if not new_image:
                continue

            domain = new_image.get("domain", {}).get("S", "unknown")
            anomaly_type = new_image.get("anomaly_type", {}).get("S", "unknown")
            severity = new_image.get("severity", {}).get("S", "low")
            resolved = new_image.get("resolved", {}).get("BOOL", False)

            if resolved:
                continue

            if severity.lower() not in ("critical", "high"):
                skipped_severity += 1
                continue

            if anomaly_type.startswith("alert_sent#"):
                continue

            if is_alert_already_sent(domain, anomaly_type):
                logger.info(f"Dedup: skipping alert for {domain}/{anomaly_type} - already sent this hour")
                skipped_dedup += 1
                continue

            sns_arn = DOMAIN_SNS_MAP.get(domain)
            if not sns_arn:
                logger.warning(f"No SNS ARN for domain: {domain}")
                continue

            subject = f"[ACIP {ENVIRONMENT.upper()}] {severity.upper()} - {domain} {anomaly_type}"
            message = build_alert_message(new_image)

            sns_client.publish(
                TopicArn=sns_arn,
                Subject=subject[:100],
                Message=message,
            )

            mark_alert_sent(domain, anomaly_type)

            logger.info(f"Alert published: domain={domain} type={anomaly_type} severity={severity}")
            processed += 1

        except Exception as e:
            logger.error(f"Failed to process record: {e}")
            errors += 1

    logger.info(f"Done: processed={processed} skipped_dedup={skipped_dedup} skipped_severity={skipped_severity} errors={errors}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "skipped_dedup": skipped_dedup,
            "skipped_severity": skipped_severity,
            "errors": errors,
        }),
    }

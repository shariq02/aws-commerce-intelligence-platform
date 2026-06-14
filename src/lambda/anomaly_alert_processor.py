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

DOMAIN_SNS_MAP = {
    "ecommerce": SNS_ECOMMERCE_ARN,
    "pharmacy": SNS_PHARMACY_ARN,
    "marketplace": SNS_MARKETPLACE_ARN,
}


def get_severity_emoji(severity):
    mapping = {"critical": "CRITICAL", "high": "HIGH", "medium": "MEDIUM", "low": "LOW"}
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
        f"Severity:     {get_severity_emoji(severity)}",
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
        lines.append(f"Event Count:  {count}")
        lines.append(f"Expected Mean: {mean}")
        lines.append(f"Threshold:    {threshold}")

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
    lines.append("This is an automated alert from ACIP pipeline.")
    return "\n".join(lines)


def lambda_handler(event, context):
    logger.info(f"Processing {len(event.get('Records', []))} DynamoDB stream records")

    processed = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            if record.get("eventName") not in ("INSERT", "MODIFY"):
                continue

            new_image = record.get("dynamodb", {}).get("NewImage", {})
            if not new_image:
                continue

            domain = new_image.get("domain", {}).get("S", "unknown")
            severity = new_image.get("severity", {}).get("S", "low")
            resolved = new_image.get("resolved", {}).get("BOOL", False)

            if resolved:
                logger.info(f"Skipping resolved anomaly for domain {domain}")
                continue

            if severity.lower() not in ("critical", "high"):
                logger.info(f"Skipping low severity anomaly: {severity}")
                continue

            sns_arn = DOMAIN_SNS_MAP.get(domain)
            if not sns_arn:
                logger.warning(f"No SNS ARN configured for domain: {domain}")
                continue

            anomaly_type = new_image.get("anomaly_type", {}).get("S", "unknown")
            subject = f"[ACIP {ENVIRONMENT.upper()}] {severity.upper()} - {domain} {anomaly_type}"
            message = build_alert_message(new_image)

            sns_client.publish(
                TopicArn=sns_arn,
                Subject=subject[:100],
                Message=message,
            )

            logger.info(f"Alert published: domain={domain} type={anomaly_type} severity={severity}")
            processed += 1

        except Exception as e:
            logger.error(f"Failed to process record: {e}")
            errors += 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "errors": errors,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }),
    }
import json
import logging
import boto3
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SNS_PHARMACY_ARN = os.environ.get("SNS_PHARMACY_ARN", "")
SNS_MARKETPLACE_ARN = os.environ.get("SNS_MARKETPLACE_ARN", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

sns_client = boto3.client("sns", region_name="eu-central-1")

DOMAIN_SNS_MAP = {
    "pharmacy": SNS_PHARMACY_ARN,
    "marketplace": SNS_MARKETPLACE_ARN,
}


def build_inventory_alert_message(record):
    domain = record.get("domain", {}).get("S", "unknown")
    product_id = record.get("product_id", {}).get("S", "unknown")
    stock_level = record.get("stock_level", {}).get("N", "N/A")
    reorder_threshold = record.get("reorder_threshold", {}).get("N", "N/A")
    days_of_supply = record.get("days_of_supply", {}).get("N", "N/A")
    alert_level = record.get("alert_level", {}).get("S", "unknown")
    updated_at = record.get("updated_at", {}).get("S", "unknown")

    lines = [
        f"AWS Commerce Intelligence Platform - Inventory Alert",
        f"Environment: {ENVIRONMENT}",
        f"",
        f"Alert Level:       {alert_level.upper()}",
        f"Domain:            {domain}",
        f"Product ID:        {product_id}",
        f"Current Stock:     {stock_level}",
        f"Reorder Threshold: {reorder_threshold}",
        f"Days of Supply:    {days_of_supply}",
        f"Last Updated:      {updated_at}",
        f"",
    ]

    if alert_level == "critical":
        lines.append("ACTION REQUIRED: Stock critically low.")
        lines.append("Immediate restock order recommended.")
    elif alert_level == "high":
        lines.append("WARNING: Stock below reorder threshold.")
        lines.append("Restock order recommended within 24 hours.")
    elif alert_level == "medium":
        lines.append("NOTICE: Stock approaching reorder threshold.")
        lines.append("Monitor closely and plan restock order.")

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
            alert_level = new_image.get("alert_level", {}).get("S", "normal")

            if alert_level.lower() not in ("critical", "high"):
                logger.info(f"Skipping alert level: {alert_level}")
                continue

            sns_arn = DOMAIN_SNS_MAP.get(domain)
            if not sns_arn:
                logger.warning(f"No SNS ARN configured for domain: {domain}")
                continue

            product_id = new_image.get("product_id", {}).get("S", "unknown")
            subject = (
                f"[ACIP {ENVIRONMENT.upper()}] {alert_level.upper()} "
                f"- Inventory Alert {domain} {product_id[:20]}"
            )
            message = build_inventory_alert_message(new_image)

            sns_client.publish(
                TopicArn=sns_arn,
                Subject=subject[:100],
                Message=message,
            )

            logger.info(
                f"Inventory alert published: domain={domain} "
                f"product={product_id} level={alert_level}"
            )
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
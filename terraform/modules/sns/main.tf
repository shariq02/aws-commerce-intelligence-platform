# SNS Module
# Creates alert topics per domain plus platform-wide topic

resource "aws_sns_topic" "ecommerce_anomalies" {
  name = "${var.project_name}-${var.environment}-anomalies-ecommerce"
}

resource "aws_sns_topic" "pharmacy_anomalies" {
  name = "${var.project_name}-${var.environment}-anomalies-pharmacy"
}

resource "aws_sns_topic" "marketplace_anomalies" {
  name = "${var.project_name}-${var.environment}-anomalies-marketplace"
}

resource "aws_sns_topic" "platform_alerts" {
  name = "${var.project_name}-${var.environment}-platform-alerts"
}
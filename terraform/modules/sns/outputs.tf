# SNS Module Outputs

output "anomaly_topic_arns" {
  description = "SNS topic ARNs per domain"
  value = {
    ecommerce   = aws_sns_topic.ecommerce_anomalies.arn
    pharmacy    = aws_sns_topic.pharmacy_anomalies.arn
    marketplace = aws_sns_topic.marketplace_anomalies.arn
    platform    = aws_sns_topic.platform_alerts.arn
  }
}
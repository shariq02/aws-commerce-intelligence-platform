# DynamoDB Module Outputs

output "table_names" {
  description = "All DynamoDB table names"
  value = {
    domain_realtime_metrics = aws_dynamodb_table.domain_realtime_metrics.name
    anomaly_flags           = aws_dynamodb_table.anomaly_flags.name
    inventory_alerts        = aws_dynamodb_table.inventory_alerts.name
    seller_sla_status       = aws_dynamodb_table.seller_sla_status.name
    platform_dlq_status     = aws_dynamodb_table.platform_dlq_status.name
  }
}
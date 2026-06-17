# Lambda Module Outputs

output "function_names" {
  description = "Lambda function names"
  value = {
    anomaly_alert_processor   = aws_lambda_function.anomaly_alert_processor.function_name
    inventory_alert_processor = aws_lambda_function.inventory_alert_processor.function_name
  }
}
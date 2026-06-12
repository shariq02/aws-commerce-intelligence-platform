# CloudWatch Module Outputs

output "flink_log_group" {
  description = "Flink CloudWatch log group name"
  value       = aws_cloudwatch_log_group.flink.name
}

output "lambda_log_group" {
  description = "Lambda CloudWatch log group name"
  value       = aws_cloudwatch_log_group.lambda.name
}
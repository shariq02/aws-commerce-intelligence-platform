# AWS Commerce Intelligence Platform
# Terraform Outputs

output "bronze_bucket_name" {
  description = "S3 Bronze bucket name"
  value       = module.s3.bronze_bucket_name
}

output "dynamodb_table_names" {
  description = "DynamoDB table names"
  value       = module.dynamodb.table_names
}

output "sns_topic_arns" {
  description = "SNS topic ARNs"
  value       = module.sns.anomaly_topic_arns
}

output "glue_registry_name" {
  description = "Glue Schema Registry name"
  value       = module.glue.registry_name
}

output "lambda_function_names" {
  description = "Lambda function names"
  value       = module.lambda.function_names
}

output "dynamodb_stream_arns" {
  description = "DynamoDB Stream ARNs for Lambda triggers"
  value = {
    anomaly_flags    = module.dynamodb.anomaly_flags_stream_arn
    inventory_alerts = module.dynamodb.inventory_alerts_stream_arn
  }
}
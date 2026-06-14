# Lambda Module Variables

variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "lambda_role_arn" {
  description = "IAM role ARN for Lambda execution"
  type        = string
}

variable "sns_anomaly_arns" {
  description = "SNS topic ARNs per domain"
  type        = map(string)
}

variable "anomaly_flags_stream_arn" {
  description = "DynamoDB Stream ARN for anomaly flags table"
  type        = string
}

variable "inventory_alerts_stream_arn" {
  description = "DynamoDB Stream ARN for inventory alerts table"
  type        = string
}
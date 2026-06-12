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
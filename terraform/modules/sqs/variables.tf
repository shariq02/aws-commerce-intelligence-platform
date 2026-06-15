# SQS Module Variables

variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "sns_ecommerce_arn" {
  description = "SNS topic ARN for ecommerce anomalies"
  type        = string
}

variable "sns_pharmacy_arn" {
  description = "SNS topic ARN for pharmacy anomalies"
  type        = string
}

variable "sns_marketplace_arn" {
  description = "SNS topic ARN for marketplace anomalies"
  type        = string
}

variable "sns_platform_arn" {
  description = "SNS topic ARN for platform alerts"
  type        = string
}
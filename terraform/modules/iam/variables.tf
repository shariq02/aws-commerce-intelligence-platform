# IAM Module Variables

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

variable "s3_bucket_arn" {
  description = "Bronze S3 bucket ARN"
  type        = string
}
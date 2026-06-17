# S3 Module Outputs

output "bronze_bucket_name" {
  description = "Bronze S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "bronze_bucket_arn" {
  description = "Bronze S3 bucket ARN"
  value       = aws_s3_bucket.bronze.arn
}
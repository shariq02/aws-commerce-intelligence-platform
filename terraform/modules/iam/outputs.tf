# IAM Module Outputs

output "lambda_role_arn" {
  description = "Lambda execution role ARN"
  value       = aws_iam_role.lambda_role.arn
}

output "app_role_arn" {
  description = "Application role ARN for Flink and generator"
  value       = aws_iam_role.app_role.arn
}
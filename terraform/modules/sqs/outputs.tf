# SQS Module Outputs

output "alert_queue_url" {
  description = "SQS alert queue URL"
  value       = aws_sqs_queue.alert_queue.id
}

output "alert_queue_arn" {
  description = "SQS alert queue ARN"
  value       = aws_sqs_queue.alert_queue.arn
}
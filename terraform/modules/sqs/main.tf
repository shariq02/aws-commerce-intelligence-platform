# SQS Module
# Creates alert queue subscribed to all SNS alert topics

resource "aws_sqs_queue" "alert_queue" {
  name                       = "${var.project_name}-${var.environment}-alert-queue"
  message_retention_seconds  = 1209600
  visibility_timeout_seconds = 60
  receive_wait_time_seconds  = 20

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_sqs_queue_policy" "alert_queue_policy" {
  queue_url = aws_sqs_queue.alert_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.alert_queue.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = "arn:aws:sns:eu-central-1:${var.aws_account_id}:${var.project_name}-${var.environment}-*"
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "ecommerce_to_sqs" {
  topic_arn = var.sns_ecommerce_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.alert_queue.arn
}

resource "aws_sns_topic_subscription" "pharmacy_to_sqs" {
  topic_arn = var.sns_pharmacy_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.alert_queue.arn
}

resource "aws_sns_topic_subscription" "marketplace_to_sqs" {
  topic_arn = var.sns_marketplace_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.alert_queue.arn
}

resource "aws_sns_topic_subscription" "platform_to_sqs" {
  topic_arn = var.sns_platform_arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.alert_queue.arn
}
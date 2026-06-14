# Lambda Module
# Deploys Lambda functions with real implementation code
# Triggered by DynamoDB Streams on anomaly and inventory tables

data "archive_file" "anomaly_alert_zip" {
  type        = "zip"
  source_file = "${path.root}/../src/lambda/anomaly_alert_processor.py"
  output_path = "${path.module}/placeholder/anomaly_alert_processor.zip"
}

data "archive_file" "inventory_alert_zip" {
  type        = "zip"
  source_file = "${path.root}/../src/lambda/inventory_alert_processor.py"
  output_path = "${path.module}/placeholder/inventory_alert_processor.zip"
}

resource "aws_lambda_function" "anomaly_alert_processor" {
  filename         = data.archive_file.anomaly_alert_zip.output_path
  function_name    = "${var.project_name}-${var.environment}-anomaly-alert-processor"
  role             = var.lambda_role_arn
  handler          = "anomaly_alert_processor.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.anomaly_alert_zip.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      SNS_ECOMMERCE_ARN   = var.sns_anomaly_arns["ecommerce"]
      SNS_PHARMACY_ARN    = var.sns_anomaly_arns["pharmacy"]
      SNS_MARKETPLACE_ARN = var.sns_anomaly_arns["marketplace"]
      ENVIRONMENT         = var.environment
    }
  }
}

resource "aws_lambda_function" "inventory_alert_processor" {
  filename         = data.archive_file.inventory_alert_zip.output_path
  function_name    = "${var.project_name}-${var.environment}-inventory-alert-processor"
  role             = var.lambda_role_arn
  handler          = "inventory_alert_processor.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.inventory_alert_zip.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      SNS_PHARMACY_ARN    = var.sns_anomaly_arns["pharmacy"]
      SNS_MARKETPLACE_ARN = var.sns_anomaly_arns["marketplace"]
      ENVIRONMENT         = var.environment
    }
  }
}

resource "aws_lambda_event_source_mapping" "anomaly_flags_trigger" {
  event_source_arn  = var.anomaly_flags_stream_arn
  function_name     = aws_lambda_function.anomaly_alert_processor.arn
  starting_position = "LATEST"
  batch_size        = 10
  enabled           = true
}

resource "aws_lambda_event_source_mapping" "inventory_alerts_trigger" {
  event_source_arn  = var.inventory_alerts_stream_arn
  function_name     = aws_lambda_function.inventory_alert_processor.arn
  starting_position = "LATEST"
  batch_size        = 10
  enabled           = true
}
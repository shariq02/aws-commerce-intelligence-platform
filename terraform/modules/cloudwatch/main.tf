# CloudWatch Module
# Creates log groups and metric alarms for pipeline monitoring

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/acip/${var.environment}/flink"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/acip/${var.environment}/lambda"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "generator" {
  name              = "/acip/${var.environment}/generator"
  retention_in_days = 7
}

resource "aws_cloudwatch_metric_alarm" "dlq_rate_high" {
  alarm_name          = "${var.project_name}-${var.environment}-dlq-rate-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DLQRatePct"
  namespace           = "ACIP/Pipeline"
  period              = 600
  statistic           = "Average"
  threshold           = 5
  alarm_description   = "DLQ rate exceeded 5 percent for any domain"
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "anomaly_flag_high" {
  alarm_name          = "${var.project_name}-${var.environment}-anomaly-flag-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "AnomalyFlagCount"
  namespace           = "ACIP/Pipeline"
  period              = 600
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "Anomaly flag count exceeded 10 in a 10-minute period"
  treat_missing_data  = "notBreaching"
}
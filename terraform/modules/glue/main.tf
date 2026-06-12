# Glue Module
# Creates Schema Registry for Kafka topic schema governance

resource "aws_glue_registry" "acip" {
  registry_name = "${var.project_name}-${var.environment}-schema-registry"

  description = "Schema registry for AWS Commerce Intelligence Platform Kafka topics"
}
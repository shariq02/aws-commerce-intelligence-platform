# Glue Module Outputs

output "registry_name" {
  description = "Glue Schema Registry name"
  value       = aws_glue_registry.acip.registry_name
}

output "registry_arn" {
  description = "Glue Schema Registry ARN"
  value       = aws_glue_registry.acip.arn
}
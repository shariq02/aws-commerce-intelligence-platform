# AWS Commerce Intelligence Platform
# Root Terraform Module

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

module "s3" {
  source       = "./modules/s3"
  project_name = var.project_name
  environment  = var.environment
}

module "dynamodb" {
  source       = "./modules/dynamodb"
  project_name = var.project_name
  environment  = var.environment
}

module "sns" {
  source       = "./modules/sns"
  project_name = var.project_name
  environment  = var.environment
}

module "glue" {
  source       = "./modules/glue"
  project_name = var.project_name
  environment  = var.environment
}

module "cloudwatch" {
  source       = "./modules/cloudwatch"
  project_name = var.project_name
  environment  = var.environment
}

module "iam" {
  source         = "./modules/iam"
  project_name   = var.project_name
  environment    = var.environment
  aws_account_id = var.aws_account_id
  s3_bucket_arn  = module.s3.bronze_bucket_arn
}

module "lambda" {
  source                      = "./modules/lambda"
  project_name                = var.project_name
  environment                 = var.environment
  lambda_role_arn             = module.iam.lambda_role_arn
  sns_anomaly_arns            = module.sns.anomaly_topic_arns
  anomaly_flags_stream_arn    = module.dynamodb.anomaly_flags_stream_arn
  inventory_alerts_stream_arn = module.dynamodb.inventory_alerts_stream_arn
}
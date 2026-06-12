# ====================================================================
# Configuration Settings for AWS Commerce Intelligence Platform
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: config.py (Project Root)
# Purpose: Centralize all configuration settings
# ====================================================================
"""
Configuration settings for AWS Commerce Intelligence Platform.
ALL SENSITIVE DATA IN .env FILE - NEVER COMMIT .env TO GITHUB

This file manages:
- PostgreSQL connection settings
- Redpanda/Kafka connection settings
- AWS service configuration
- Flink configuration
- Databricks configuration
- Prefect configuration
- FastAPI configuration
- Data generator configuration
- Layer-specific settings (Bronze, Silver, Gold)
- Logging configuration

Usage:
    from config import get_database_url, KAFKA_CONFIG, AWS_CONFIG
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# ====================================================================
# PROJECT ROOT
# ====================================================================

PROJECT_ROOT = Path(__file__).parent
PROJECT_NAME = os.getenv('PROJECT_NAME', 'aws-commerce-intelligence-platform')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================

# Source directories
SRC_DIR = PROJECT_ROOT / "src"
GENERATOR_DIR = SRC_DIR / "generator"
FLINK_DIR = SRC_DIR / "flink"
LAMBDA_DIR = SRC_DIR / "lambda"

# Application directories
FASTAPI_DIR = PROJECT_ROOT / "fastapi"
DBT_DIR = PROJECT_ROOT / "dbt"
DATABRICKS_DIR = PROJECT_ROOT / "databricks"
TERRAFORM_DIR = PROJECT_ROOT / "terraform"
PREFECT_DIR = PROJECT_ROOT / "prefect"
GRAFANA_DIR = PROJECT_ROOT / "grafana"

# SQL directories
SQL_DIR = PROJECT_ROOT / "sql"
SQL_DDL_DIR = SQL_DIR / "ddl"
SQL_VIEWS_DIR = SQL_DIR / "views"
SQL_QUERIES_DIR = SQL_DIR / "queries"

# Data directories (gitignored)
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_RAW_OLIST_DIR = DATA_RAW_DIR / "olist"
DATA_RAW_PHARMA_DIR = DATA_RAW_DIR / "pharma"

# Test and scripts directories
TESTS_DIR = PROJECT_ROOT / "tests"
TESTS_GENERATOR_DIR = TESTS_DIR / "generator"
TESTS_FLINK_DIR = TESTS_DIR / "flink"
TESTS_API_DIR = TESTS_DIR / "api"
TESTS_INTEGRATION_DIR = TESTS_DIR / "integration"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SCRIPTS_SETUP_DIR = SCRIPTS_DIR / "setup"
SCRIPTS_UTILS_DIR = SCRIPTS_DIR / "utils"
SCRIPTS_TESTS_DIR = SCRIPTS_DIR / "tests"

# Docs directory
DOCS_DIR = PROJECT_ROOT / "docs"

# Logs directory
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_GENERATOR_DIR = LOGS_DIR / "generator"
LOGS_FLINK_DIR = LOGS_DIR / "flink"
LOGS_API_DIR = LOGS_DIR / "api"
LOGS_PIPELINE_DIR = LOGS_DIR / "pipeline"
LOGS_PREFECT_DIR = LOGS_DIR / "prefect"

# Flink checkpoint directory
FLINK_CHECKPOINT_DIR = Path(os.getenv('FLINK_CHECKPOINT_DIR', '/tmp/flink-checkpoints'))

# Auto-create runtime directories on import
for directory in [
    DATA_RAW_OLIST_DIR,
    DATA_RAW_PHARMA_DIR,
    TESTS_GENERATOR_DIR,
    TESTS_FLINK_DIR,
    TESTS_API_DIR,
    TESTS_INTEGRATION_DIR,
    SCRIPTS_SETUP_DIR,
    SCRIPTS_UTILS_DIR,
    SCRIPTS_TESTS_DIR,
    LOGS_GENERATOR_DIR,
    LOGS_FLINK_DIR,
    LOGS_API_DIR,
    LOGS_PIPELINE_DIR,
    LOGS_PREFECT_DIR,
    FLINK_CHECKPOINT_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# POSTGRESQL DATABASE
# ====================================================================

DATABASE_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', '172.31.32.1'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'acip'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}


def get_database_url() -> str:
    """Return SQLAlchemy-compatible PostgreSQL connection URL."""
    cfg = DATABASE_CONFIG
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )


# ====================================================================
# REDPANDA / KAFKA CONFIGURATION
# ====================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client_id': os.getenv('KAFKA_CLIENT_ID', 'acip-producer'),
    'compression_type': os.getenv('KAFKA_COMPRESSION_TYPE', 'gzip'),
    'acks': os.getenv('KAFKA_ACKS', 'all'),
    'retries': int(os.getenv('KAFKA_RETRIES', 3)),
    'batch_size': int(os.getenv('KAFKA_BATCH_SIZE', 16384)),
    'linger_ms': int(os.getenv('KAFKA_LINGER_MS', 10)),
}

KAFKA_TOPICS = {
    'ecommerce': os.getenv('TOPIC_ECOMMERCE', 'ecommerce.events'),
    'pharmacy': os.getenv('TOPIC_PHARMACY', 'pharmacy.events'),
    'marketplace': os.getenv('TOPIC_MARKETPLACE', 'marketplace.events'),
    'anomalies': os.getenv('TOPIC_ANOMALIES', 'platform.anomalies'),
    'dlq': os.getenv('TOPIC_DLQ', 'platform.dlq'),
}

KAFKA_CONSUMER_GROUPS = {
    'flink_ecommerce': 'flink-ecommerce-group',
    'flink_pharmacy': 'flink-pharmacy-group',
    'flink_marketplace': 'flink-marketplace-group',
    'flink_cross_domain': 'flink-cross-domain-group',
    'bronze_writer': 'bronze-writer-group',
}

# ====================================================================
# AWS CONFIGURATION
# ====================================================================

AWS_CONFIG = {
    'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'region': os.getenv('AWS_DEFAULT_REGION', 'eu-central-1'),
    'account_id': os.getenv('AWS_ACCOUNT_ID'),
}

# S3
S3_CONFIG = {
    'bronze_bucket': os.getenv('S3_BRONZE_BUCKET', 'acip-bronze'),
    'silver_bucket': os.getenv('S3_SILVER_BUCKET', 'acip-silver'),
    'gold_bucket': os.getenv('S3_GOLD_BUCKET', 'acip-gold'),
}

# DynamoDB
DYNAMODB_CONFIG = {
    'metrics_table': os.getenv('DYNAMODB_METRICS_TABLE', 'domain-realtime-metrics'),
    'anomalies_table': os.getenv('DYNAMODB_ANOMALIES_TABLE', 'anomaly-flags'),
    'inventory_table': os.getenv('DYNAMODB_INVENTORY_TABLE', 'inventory-alerts'),
    'seller_table': os.getenv('DYNAMODB_SELLER_TABLE', 'seller-sla-status'),
    'dlq_table': os.getenv('DYNAMODB_DLQ_TABLE', 'platform-dlq-status'),
}

# SNS
SNS_CONFIG = {
    'ecommerce_topic_arn': os.getenv('SNS_ECOMMERCE_TOPIC_ARN'),
    'pharmacy_topic_arn': os.getenv('SNS_PHARMACY_TOPIC_ARN'),
    'marketplace_topic_arn': os.getenv('SNS_MARKETPLACE_TOPIC_ARN'),
    'platform_topic_arn': os.getenv('SNS_PLATFORM_TOPIC_ARN'),
}

# Glue Schema Registry
GLUE_CONFIG = {
    'registry_name': os.getenv('GLUE_REGISTRY_NAME', 'acip-schema-registry'),
    'registry_arn': os.getenv('GLUE_REGISTRY_ARN'),
}

# ====================================================================
# FLINK CONFIGURATION
# ====================================================================

FLINK_CONFIG = {
    'home': os.getenv('FLINK_HOME', '/home/sharique/flink'),
    'checkpoint_dir': os.getenv('FLINK_CHECKPOINT_DIR', '/tmp/flink-checkpoints'),
    'checkpoint_interval_ms': int(os.getenv('FLINK_CHECKPOINT_INTERVAL', 60000)),
    'watermark_delay_ms': int(os.getenv('FLINK_WATERMARK_DELAY', 300000)),
    'parallelism': 1,
}

# Window configurations
FLINK_WINDOWS = {
    'ecommerce_volume_window_minutes': 10,
    'pharmacy_velocity_window_minutes': 15,
    'pharmacy_velocity_slide_minutes': 5,
    'cross_domain_window_hours': 1,
    'anomaly_detection_window_minutes': 10,
    'seller_session_gap_minutes': 30,
    'late_event_tolerance_minutes': 5,
}

# ====================================================================
# DATABRICKS CONFIGURATION
# ====================================================================

DATABRICKS_CONFIG = {
    'host': os.getenv('DATABRICKS_HOST', 'https://community.cloud.databricks.com'),
    'token': os.getenv('DATABRICKS_TOKEN'),
    'catalog': 'acip',
    'schemas': {
        'bronze': 'bronze',
        'silver': 'silver',
        'gold': 'gold',
        'quality': 'quality',
    },
    'dbfs_paths': {
        'raw_olist': '/FileStore/raw/olist/',
        'raw_pharma': '/FileStore/raw/pharma/',
        'bronze': '/FileStore/bronze/',
        'silver': '/FileStore/silver/',
        'gold': '/FileStore/gold/',
    },
}

# ====================================================================
# PREFECT CONFIGURATION
# ====================================================================

PREFECT_CONFIG = {
    'api_url': os.getenv('PREFECT_API_URL'),
    'api_key': os.getenv('PREFECT_API_KEY'),
    'schedules': {
        'batch_ingestion': '0 1 * * *',
        'silver_transform': '0 2 * * *',
        'gold_build': '0 3 * * *',
        'dbt_run': '0 4 * * *',
        'quality_report': '0 5 * * *',
    },
}

# ====================================================================
# FASTAPI CONFIGURATION
# ====================================================================

FASTAPI_CONFIG = {
    'host': os.getenv('FASTAPI_HOST', '0.0.0.0'),
    'port': int(os.getenv('FASTAPI_PORT', 8000)),
    'reload': os.getenv('FASTAPI_RELOAD', 'true').lower() == 'true',
    'title': 'AWS Commerce Intelligence Platform API',
    'version': '1.0.0',
}

# ====================================================================
# DATA GENERATOR CONFIGURATION
# ====================================================================

GENERATOR_CONFIG = {
    'events_per_second': int(os.getenv('GENERATOR_EVENTS_PER_SECOND', 10)),
    'replay_speed': int(os.getenv('GENERATOR_REPLAY_SPEED', 60)),
    'anomaly_mode': os.getenv('GENERATOR_ANOMALY_MODE', 'false').lower() == 'true',
    'anomaly_probability': float(os.getenv('GENERATOR_ANOMALY_PROBABILITY', 0.05)),
    'domains': ['ecommerce', 'pharmacy', 'marketplace'],
}

# ====================================================================
# BRONZE LAYER CONFIGURATION
# ====================================================================

BRONZE_CONFIG = {
    'poll_timeout': int(os.getenv('BRONZE_POLL_TIMEOUT', 1000)),
    'max_poll_records': int(os.getenv('BRONZE_MAX_POLL_RECORDS', 500)),
    'auto_offset_reset': os.getenv('BRONZE_AUTO_OFFSET_RESET', 'earliest'),
    'parquet_batch_size': int(os.getenv('BRONZE_PARQUET_BATCH_SIZE', 1000)),
    'write_interval': int(os.getenv('BRONZE_WRITE_INTERVAL', 60)),
    'compression': os.getenv('BRONZE_PARQUET_COMPRESSION', 'snappy'),
    'checkpoint_interval': int(os.getenv('BRONZE_CHECKPOINT_INTERVAL', 300)),
    's3_partition_keys': ['domain', 'event_type', 'year', 'month', 'day', 'hour'],
}

# ====================================================================
# SILVER LAYER CONFIGURATION
# ====================================================================

SILVER_CONFIG = {
    'completeness_threshold': float(os.getenv('SILVER_COMPLETENESS_THRESHOLD', 0.95)),
    'uniqueness_threshold': float(os.getenv('SILVER_UNIQUENESS_THRESHOLD', 1.0)),
    'batch_size': int(os.getenv('SILVER_BATCH_SIZE', 10000)),
    'primary_keys': {
        'ecommerce': ['order_id', 'product_id'],
        'pharmacy': ['transaction_id'],
        'marketplace': ['listing_id', 'order_id'],
    },
}

# ====================================================================
# GOLD LAYER CONFIGURATION
# ====================================================================

GOLD_CONFIG = {
    'snapshot_interval_hours': int(os.getenv('GOLD_SNAPSHOT_INTERVAL_HOURS', 1)),
    'retention_days': int(os.getenv('GOLD_RETENTION_DAYS', 90)),
    'aggregation_batch_size': int(os.getenv('GOLD_AGGREGATION_BATCH_SIZE', 50000)),
    'scd2_tables': ['dim_customer', 'dim_seller'],
    'scd1_tables': ['dim_product'],
    'static_tables': ['dim_date', 'dim_geography', 'dim_domain'],
}

# ====================================================================
# ANOMALY DETECTION CONFIGURATION
# ====================================================================

ANOMALY_CONFIG = {
    'volume_spike_std_multiplier': 2.0,
    'inventory_critical_days': 3,
    'inventory_high_days': 7,
    'inventory_medium_days': 14,
    'seller_sla_breach_threshold': 0.20,
    'price_change_threshold_pct': 20.0,
    'dlq_rate_threshold_pct': 5.0,
}

# ====================================================================
# DOMAINS CONFIGURATION
# ====================================================================

DOMAINS = {
    'ecommerce': {
        'name': 'E-Commerce Orders',
        'topic': KAFKA_TOPICS['ecommerce'],
        'event_types': ['order.placed', 'order.fulfilled', 'order.returned'],
    },
    'pharmacy': {
        'name': 'Pharmacy and Health Retail',
        'topic': KAFKA_TOPICS['pharmacy'],
        'event_types': [
            'prescription.submitted',
            'prescription.filled',
            'inventory.updated',
        ],
    },
    'marketplace': {
        'name': 'Marketplace Seller Operations',
        'topic': KAFKA_TOPICS['marketplace'],
        'event_types': [
            'listing.created',
            'seller.order.dispatched',
            'price.updated',
        ],
    },
}

# ====================================================================
# LOGGING CONFIGURATION
# ====================================================================

LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s [%(levelname)8s] %(name)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'app_log': str(LOGS_DIR / 'app.log'),
    'error_log': str(LOGS_DIR / 'error.log'),
    'generator_log': str(LOGS_GENERATOR_DIR / 'generator.log'),
    'flink_log': str(LOGS_FLINK_DIR / 'flink.log'),
    'api_log': str(LOGS_API_DIR / 'api.log'),
    'pipeline_log': str(LOGS_PIPELINE_DIR / 'pipeline.log'),
    'prefect_log': str(LOGS_PREFECT_DIR / 'prefect.log'),
}


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger for the given module name.
    Routes logs to appropriate layer-specific log files.

    Usage:
        from config import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    level = getattr(logging, LOGGING_CONFIG['level'].upper(), logging.INFO)
    fmt = logging.Formatter(
        LOGGING_CONFIG['format'],
        datefmt=LOGGING_CONFIG['date_format']
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # Master app log handler
    app_handler = RotatingFileHandler(
        LOGGING_CONFIG['app_log'],
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    app_handler.setLevel(level)
    app_handler.setFormatter(fmt)

    # Error log handler
    error_handler = logging.FileHandler(
        LOGGING_CONFIG['error_log'],
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(fmt)

    # Layer-specific log routing
    layer_log_file = None

    if 'generator' in name:
        layer_log_file = LOGGING_CONFIG['generator_log']
    elif 'flink' in name:
        layer_log_file = LOGGING_CONFIG['flink_log']
    elif 'api' in name or 'fastapi' in name or 'router' in name:
        layer_log_file = LOGGING_CONFIG['api_log']
    elif 'prefect' in name or 'flow' in name:
        layer_log_file = LOGGING_CONFIG['prefect_log']
    elif 'pipeline' in name or 'orchestrat' in name:
        layer_log_file = LOGGING_CONFIG['pipeline_log']

    if layer_log_file:
        layer_handler = logging.FileHandler(layer_log_file, encoding='utf-8')
        layer_handler.setLevel(level)
        layer_handler.setFormatter(fmt)
        logger.addHandler(layer_handler)

    logger.setLevel(level)
    logger.addHandler(ch)
    logger.addHandler(app_handler)
    logger.addHandler(error_handler)
    logger.propagate = False

    return logger


# ====================================================================
# VALIDATION
# ====================================================================

def validate_config() -> bool:
    """Validate critical configuration settings."""
    errors = []

    if not DATABASE_CONFIG['password']:
        errors.append("POSTGRES_PASSWORD not set in .env")

    if not KAFKA_CONFIG['bootstrap_servers']:
        errors.append("KAFKA_BOOTSTRAP_SERVERS not set in .env")

    if not AWS_CONFIG['access_key_id']:
        errors.append("AWS_ACCESS_KEY_ID not set in .env")

    if not AWS_CONFIG['secret_access_key']:
        errors.append("AWS_SECRET_ACCESS_KEY not set in .env")

    if not DATABRICKS_CONFIG['token']:
        errors.append("DATABRICKS_TOKEN not set in .env")

    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True


# ====================================================================
# MAIN - RUN DIRECTLY TO VERIFY CONFIG
# ====================================================================

if __name__ == '__main__':
    print("=" * 70)
    print("AWS COMMERCE INTELLIGENCE PLATFORM - CONFIGURATION")
    print("=" * 70)
    print(f"Project root:        {PROJECT_ROOT}")
    print(f"Environment:         {ENVIRONMENT}")
    print(f"Database:            {DATABASE_CONFIG['database']}")
    print(f"Database host:       {DATABASE_CONFIG['host']}")
    print(f"Kafka bootstrap:     {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"AWS region:          {AWS_CONFIG['region']}")
    print(f"S3 bronze bucket:    {S3_CONFIG['bronze_bucket']}")
    print(f"DynamoDB metrics:    {DYNAMODB_CONFIG['metrics_table']}")
    print(f"Flink checkpoint:    {FLINK_CONFIG['checkpoint_dir']}")
    print(f"Databricks host:     {DATABRICKS_CONFIG['host']}")
    print(f"Data directory:      {DATA_DIR}")
    print(f"Logs directory:      {LOGS_DIR}")

    print("\n" + "=" * 70)
    print("CONFIGURATION VALIDATION")
    print("=" * 70)

    if validate_config():
        print("Status: PASSED")
    else:
        print("Status: FAILED - Fix errors above")

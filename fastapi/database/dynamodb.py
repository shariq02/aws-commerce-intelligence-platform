# ====================================================================
# AWS Commerce Intelligence Platform - DynamoDB Connection
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/database/dynamodb.py
# Purpose: boto3 DynamoDB client and table accessors
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from config import AWS_CONFIG, DYNAMODB_CONFIG, get_logger

logger = get_logger("api.database.dynamodb")

_dynamodb_resource = None


def get_dynamodb():
    """Return a singleton DynamoDB resource."""
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource(
            "dynamodb",
            region_name=AWS_CONFIG["region"],
            aws_access_key_id=AWS_CONFIG["access_key_id"],
            aws_secret_access_key=AWS_CONFIG["secret_access_key"],
        )
    return _dynamodb_resource


def get_table(table_key: str):
    """
    Return a DynamoDB Table resource by config key.
    table_key must be one of: metrics_table, anomalies_table,
    inventory_table, seller_table, dlq_table
    """
    table_name = DYNAMODB_CONFIG.get(table_key)
    if not table_name:
        raise ValueError(f"Unknown DynamoDB table key: {table_key}")
    return get_dynamodb().Table(table_name)


def safe_query(table, **kwargs) -> list:
    """
    Execute a DynamoDB query and return items list.
    Returns empty list on error.
    """
    try:
        response = table.query(**kwargs)
        return response.get("Items", [])
    except ClientError as e:
        logger.error(f"DynamoDB query error: {e.response['Error']['Message']}")
        return []


def safe_scan(table, **kwargs) -> list:
    """
    Execute a DynamoDB scan and return items list.
    Returns empty list on error.
    """
    try:
        response = table.scan(**kwargs)
        return response.get("Items", [])
    except ClientError as e:
        logger.error(f"DynamoDB scan error: {e.response['Error']['Message']}")
        return []


def test_connection() -> bool:
    """Test DynamoDB connection on startup."""
    try:
        dynamodb = get_dynamodb()
        table_name = DYNAMODB_CONFIG["metrics_table"]
        table = dynamodb.Table(table_name)
        table.load()
        logger.info(f"DynamoDB connection: OK (table={table_name})")
        return True
    except Exception as e:
        logger.error(f"DynamoDB connection failed: {e}")
        return False

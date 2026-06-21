# ====================================================================
# AWS Commerce Intelligence Platform - Ecommerce Service
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/services/ecommerce_service.py
# Purpose: Query logic for ecommerce endpoints
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import text

from config import get_logger
from database.dynamodb import get_table, safe_scan

logger = get_logger("api.services.ecommerce")


# ---------------------------------------------------------------------------
# Analytical - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

def get_daily_order_volume(db: Session, days: int = 30) -> list:
    sql = text("""
        SELECT
            full_date,
            COUNT(transaction_key) AS order_count,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_order_value
        FROM acip_dbt_marts.mart_ecommerce_orders
        WHERE full_date IS NOT NULL
        GROUP BY full_date
        ORDER BY full_date DESC
        LIMIT :days
    """).bindparams(days=days)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_customer_segments(db: Session) -> list:
    sql = text("""
        SELECT
            customer_segment,
            COUNT(transaction_key) AS order_count,
            SUM(total_amount) AS total_revenue,
            AVG(total_amount) AS avg_order_value,
            ROUND(
                SUM(CASE WHEN return_reason IS NOT NULL THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(transaction_key), 0) * 100, 2
            ) AS return_rate
        FROM acip_dbt_marts.mart_ecommerce_orders
        WHERE customer_segment IS NOT NULL
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_fulfilment_analysis(db: Session) -> list:
    sql = text("""
        SELECT
            fulfilment_bucket,
            COUNT(transaction_key) AS order_count,
            AVG(fulfilment_time_days) AS avg_fulfilment_days,
            ROUND(
                SUM(CASE WHEN delivery_on_time = TRUE THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(transaction_key), 0) * 100, 2
            ) AS on_time_rate
        FROM acip_dbt_marts.mart_ecommerce_orders
        WHERE fulfilment_bucket IS NOT NULL
        GROUP BY fulfilment_bucket
        ORDER BY order_count DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_regional_orders(db: Session, limit: int = 10) -> list:
    sql = text("""
        SELECT
            state_region,
            state,
            COUNT(transaction_key) AS order_count,
            SUM(total_amount) AS total_revenue
        FROM acip_dbt_marts.mart_ecommerce_orders
        WHERE state_region IS NOT NULL
        GROUP BY state_region, state
        ORDER BY order_count DESC
        LIMIT :limit
    """).bindparams(limit=limit)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Real-time - DynamoDB
# ---------------------------------------------------------------------------

def get_realtime_metrics(domain: str = "ecommerce") -> list:
    table = get_table("metrics_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :domain)",
        ExpressionAttributeValues={":domain": domain},
        Limit=50,
    )


def get_anomaly_flags(domain: str = "ecommerce") -> list:
    table = get_table("anomalies_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :domain)",
        ExpressionAttributeValues={":domain": domain},
        Limit=50,
    )

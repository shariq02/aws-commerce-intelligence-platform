# ====================================================================
# AWS Commerce Intelligence Platform - Platform Service
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/services/platform_service.py
# Purpose: Query logic for platform-wide endpoints
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import text

from config import get_logger
from database.dynamodb import get_table, safe_scan

logger = get_logger("api.services.platform")


# ---------------------------------------------------------------------------
# Analytical - PostgreSQL acip_dbt_marts / acip_quality
# ---------------------------------------------------------------------------

def get_cross_domain_summary(db: Session, months: int = 12) -> list:
    sql = text("""
        SELECT
            year, month,
            ecommerce_transactions,
            ecommerce_net_revenue,
            ecommerce_avg_order_value,
            ecommerce_returns,
            pharmacy_dispensing_events,
            pharmacy_avg_fill_time,
            pharmacy_critical_stock_events,
            marketplace_dispatch_events,
            marketplace_gross_revenue,
            marketplace_avg_dispatch_days,
            marketplace_sla_breaches
        FROM acip_dbt_marts.mart_cross_domain_summary
        ORDER BY year DESC, month DESC
        LIMIT :months
    """).bindparams(months=months)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_pipeline_watermarks(db: Session) -> list:
    sql = text("""
        SELECT *
        FROM acip_quality.pipeline_watermarks
        ORDER BY last_updated_at DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_daily_domain_metrics(db: Session, days: int = 30) -> list:
    sql = text("""
        SELECT
            metric_date,
            domain,
            event_count,
            total_value
        FROM acip_gold.agg_daily_domain_metrics
        WHERE metric_date IS NOT NULL
        ORDER BY metric_date DESC, domain
        LIMIT :limit
    """).bindparams(limit=days * 3)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Real-time - DynamoDB
# ---------------------------------------------------------------------------

def get_cross_domain_realtime() -> list:
    table = get_table("metrics_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :prefix)",
        ExpressionAttributeValues={":prefix": "cross_domain"},
        Limit=50,
    )


def get_all_anomalies() -> list:
    table = get_table("anomalies_table")
    return safe_scan(table, Limit=100)

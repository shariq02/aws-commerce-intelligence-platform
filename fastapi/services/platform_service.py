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
# Analytical - PostgreSQL acip_dbt_marts / acip_gold / acip_quality
# ---------------------------------------------------------------------------

def get_cross_domain_summary(db: Session, months: int = 12) -> list:
    sql = text("""
        SELECT
            year,
            month,
            ecommerce_transactions,
            ecommerce_net_revenue,
            ecommerce_avg_order_value,
            ecommerce_returns,
            ecommerce_on_time_deliveries,
            ecommerce_avg_review_score,
            pharmacy_dispensing_events,
            pharmacy_avg_fill_time,
            pharmacy_critical_stock_events,
            pharmacy_rx_events,
            pharmacy_avg_days_of_supply,
            marketplace_dispatch_events,
            marketplace_gross_revenue,
            marketplace_net_revenue,
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
        SELECT
            run_id,
            domain,
            stage,
            component,
            completed_at,
            rows_written,
            status
        FROM acip_quality.pipeline_watermarks
        ORDER BY completed_at DESC NULLS LAST
        LIMIT 50
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_daily_domain_metrics(db: Session, days: int = 30) -> list:
    sql = text("""
        SELECT
            metric_date,
            domain,
            SUM(event_count) AS event_count,
            SUM(total_value) AS total_value
        FROM acip_gold.agg_daily_domain_metrics
        WHERE metric_date IS NOT NULL
        GROUP BY metric_date, domain
        ORDER BY metric_date DESC
        LIMIT :days
    """).bindparams(days=days)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_anomaly_rates(db: Session, domain: str = None, spikes_only: bool = False) -> list:
    """
    Daily domain anomaly rates from mart_domain_anomaly_rates.
    Serves CD-02 (cross-domain anomaly comparison) and AD-01
    (order volume spike detection, batch reframe).
    Supports optional filtering by domain and spike-only rows.
    """
    filters = []
    params = {}

    if domain:
        filters.append("domain = :domain")
        params["domain"] = domain

    if spikes_only:
        filters.append("(is_spike = TRUE OR is_drop = TRUE)")

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    sql = text(f"""
        SELECT
            domain,
            metric_date,
            event_count,
            total_value,
            rolling_mean,
            rolling_std,
            trailing_days_available,
            spike_magnitude,
            is_spike,
            is_drop
        FROM acip_dbt_marts.mart_domain_anomaly_rates
        {where_clause}
        ORDER BY metric_date DESC, domain ASC
        LIMIT 200
    """).bindparams(**params)

    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_hourly_volume(db: Session, domain: str = None) -> list:
    """
    Hourly transaction volume per domain from mart_hourly_transaction_volume.
    Batch approximation of real-time hourly tumbling window aggregation.
    Serves CD-04.
    """
    filters = []
    params = {}

    if domain:
        filters.append("domain = :domain")
        params["domain"] = domain

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    sql = text(f"""
        SELECT
            domain,
            hour_of_day,
            event_count,
            domain_total,
            pct_of_domain_total,
            time_of_day_bucket
        FROM acip_dbt_marts.mart_hourly_transaction_volume
        {where_clause}
        ORDER BY domain ASC, hour_of_day ASC
    """).bindparams(**params)

    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Real-time - DynamoDB
# ---------------------------------------------------------------------------

def get_cross_domain_realtime() -> list:
    table = get_table("metrics_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :domain)",
        ExpressionAttributeValues={":domain": "cross_domain"},
        Limit=50,
    )


def get_all_anomalies() -> list:
    table = get_table("anomalies_table")
    return safe_scan(table, Limit=100)

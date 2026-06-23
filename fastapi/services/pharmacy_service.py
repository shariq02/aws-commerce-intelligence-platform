# ====================================================================
# AWS Commerce Intelligence Platform - Pharmacy Service
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/services/pharmacy_service.py
# Purpose: Query logic for pharmacy endpoints
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from config import get_logger
from database.dynamodb import get_table, safe_scan

logger = get_logger("api.services.pharmacy")


# ---------------------------------------------------------------------------
# Analytical - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

def get_drug_category_demand(db: Session) -> list:
    sql = text("""
        SELECT
            category,
            drug_class,
            is_prescription,
            SUM(quantity) AS total_quantity,
            AVG(fill_time_mins) AS avg_fill_time_mins,
            COUNT(snapshot_key) AS event_count
        FROM acip_dbt_marts.mart_pharmacy_dispensing
        WHERE category IS NOT NULL
        GROUP BY category, drug_class, is_prescription
        ORDER BY total_quantity DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_stock_alerts(db: Session, level: str = None) -> list:
    if level:
        sql = text("""
            SELECT
                snapshot_key, product_id, category,
                stock_level, reorder_threshold, stock_buffer,
                stock_alert_level AS alert_level, requires_action
            FROM acip_dbt_marts.mart_pharmacy_dispensing
            WHERE requires_action = TRUE
            AND stock_alert_level = :level
            ORDER BY stock_buffer ASC
            LIMIT 100
        """).bindparams(level=level)
    else:
        sql = text("""
            SELECT
                snapshot_key, product_id, category,
                stock_level, reorder_threshold, stock_buffer,
                stock_alert_level AS alert_level, requires_action
            FROM acip_dbt_marts.mart_pharmacy_dispensing
            WHERE requires_action = TRUE
            ORDER BY stock_buffer ASC
            LIMIT 100
        """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_rx_otc_summary(db: Session) -> list:
    sql = text("""
        SELECT
            is_prescription,
            CASE WHEN is_prescription THEN 'Prescription (Rx)' ELSE 'OTC' END AS label,
            SUM(quantity) AS total_quantity,
            AVG(fill_time_mins) AS avg_fill_time_mins,
            COUNT(snapshot_key) AS event_count
        FROM acip_dbt_marts.mart_pharmacy_dispensing
        WHERE is_prescription IS NOT NULL
        GROUP BY is_prescription
        ORDER BY is_prescription DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_peak_hour_analysis(db: Session) -> list:
    sql = text("""
        SELECT
            hour_of_day,
            COUNT(snapshot_key) AS event_count,
            AVG(fill_time_mins) AS avg_fill_time_mins,
            SUM(CASE WHEN is_peak_hour THEN 1 ELSE 0 END) AS peak_hour_events
        FROM acip_dbt_marts.mart_pharmacy_dispensing
        WHERE hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day ASC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_reorder_alerts(
    db: Session,
    urgency_score: Optional[int] = None,
    is_prescription: Optional[bool] = None,
    limit: int = 50,
) -> list:
    """
    Pharmacy reorder alerts from mart_pharmacy_reorder_alerts.
    Returns products ordered by urgency with stock context and
    recommended actions. Supports filtering by urgency score and Rx status.
    """
    filters = []
    params = {"limit": limit}

    if urgency_score is not None:
        filters.append("urgency_score = :urgency_score")
        params["urgency_score"] = urgency_score

    if is_prescription is not None:
        filters.append("is_prescription = :is_prescription")
        params["is_prescription"] = is_prescription

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    sql = text(f"""
        SELECT
            product_id,
            category,
            category_group,
            atc_code,
            drug_class,
            is_prescription,
            current_stock_level,
            reorder_threshold,
            stock_buffer,
            current_days_of_supply,
            stock_alert_level,
            urgency_score,
            recommended_action,
            is_below_reorder,
            is_critical,
            avg_stock_level,
            min_stock_level,
            max_stock_level,
            avg_days_of_supply,
            avg_fill_time_mins,
            critical_count,
            critical_frequency_rate,
            stock_vs_avg_trend,
            overall_urgency_rank,
            last_snapshot_at
        FROM acip_dbt_marts.mart_pharmacy_reorder_alerts
        {where_clause}
        ORDER BY overall_urgency_rank ASC
        LIMIT :limit
    """).bindparams(**params)

    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Real-time - DynamoDB
# ---------------------------------------------------------------------------

def get_inventory_alerts() -> list:
    table = get_table("inventory_table")
    return safe_scan(table, Limit=50)


def get_realtime_pharmacy_metrics() -> list:
    table = get_table("metrics_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :domain)",
        ExpressionAttributeValues={":domain": "pharmacy"},
        Limit=50,
    )

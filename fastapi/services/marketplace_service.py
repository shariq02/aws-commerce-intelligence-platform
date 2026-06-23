# ====================================================================
# AWS Commerce Intelligence Platform - Marketplace Service
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/services/marketplace_service.py
# Purpose: Query logic for marketplace endpoints
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional

from config import get_logger
from database.dynamodb import get_table, safe_scan

logger = get_logger("api.services.marketplace")


# ---------------------------------------------------------------------------
# Analytical - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

def get_sla_breach_by_tier(db: Session) -> list:
    sql = text("""
        SELECT
            seller_tier,
            COUNT(performance_key) AS total_dispatches,
            SUM(CASE WHEN is_sla_breached THEN 1 ELSE 0 END) AS breached_count,
            ROUND(
                AVG(CASE WHEN is_sla_breached THEN 1.0 ELSE 0.0 END) * 100, 2
            ) AS breach_rate,
            AVG(dispatch_time_days) AS avg_dispatch_days
        FROM acip_dbt_marts.mart_marketplace_performance
        WHERE seller_tier IS NOT NULL
        GROUP BY seller_tier
        ORDER BY breach_rate DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_top_sellers(db: Session, limit: int = 20) -> list:
    sql = text("""
        SELECT
            seller_id,
            seller_tier,
            COUNT(performance_key) AS total_dispatches,
            ROUND(
                AVG(CASE WHEN is_sla_breached THEN 1.0 ELSE 0.0 END) * 100, 2
            ) AS breach_rate,
            AVG(dispatch_time_days) AS avg_dispatch_days,
            SUM(gross_revenue) AS gross_revenue,
            SUM(net_revenue) AS net_revenue
        FROM acip_dbt_marts.mart_marketplace_performance
        WHERE seller_id IS NOT NULL
        GROUP BY seller_id, seller_tier
        ORDER BY gross_revenue DESC
        LIMIT :limit
    """).bindparams(limit=limit)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_price_volatility(db: Session) -> list:
    sql = text("""
        SELECT
            category,
            AVG(change_pct) AS avg_change_pct,
            MAX(change_pct) AS max_change_pct,
            COUNT(performance_key) AS price_change_events,
            SUM(CASE WHEN ABS(change_pct) >= 20 THEN 1 ELSE 0 END) AS significant_changes
        FROM acip_dbt_marts.mart_marketplace_performance
        WHERE change_pct IS NOT NULL
        AND category IS NOT NULL
        GROUP BY category
        ORDER BY avg_change_pct DESC
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_freight_burden(db: Session) -> list:
    sql = text("""
        SELECT
            freight_burden,
            seller_tier,
            COUNT(performance_key) AS dispatch_count,
            AVG(freight_value / NULLIF(price, 0)) AS avg_freight_ratio,
            AVG(dispatch_time_days) AS avg_dispatch_days
        FROM acip_dbt_marts.mart_marketplace_performance
        WHERE freight_burden IS NOT NULL
        AND seller_tier IS NOT NULL
        GROUP BY freight_burden, seller_tier
        ORDER BY freight_burden, seller_tier
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_dispatch_speed_distribution(db: Session) -> list:
    sql = text("""
        SELECT
            dispatch_speed_bucket,
            seller_tier,
            COUNT(performance_key) AS dispatch_count,
            AVG(dispatch_time_days) AS avg_dispatch_days
        FROM acip_dbt_marts.mart_marketplace_performance
        WHERE dispatch_speed_bucket IS NOT NULL
        GROUP BY dispatch_speed_bucket, seller_tier
        ORDER BY dispatch_speed_bucket, seller_tier
    """)
    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


def get_seller_leaderboard(
    db: Session,
    tier: Optional[str] = None,
    rank_by: str = "overall",
    limit: int = 20,
) -> list:
    """
    Seller leaderboard from mart_seller_leaderboard.
    Supports filtering by tier and sorting by different rank dimensions.
    """
    rank_column_map = {
        "overall":        "overall_rank",
        "revenue":        "rank_by_revenue",
        "sla_compliance": "rank_by_sla_compliance",
        "volume":         "rank_by_volume",
        "speed":          "rank_by_speed",
    }
    order_col = rank_column_map.get(rank_by, "overall_rank")

    filters = []
    params = {"limit": limit}

    if tier:
        filters.append("seller_tier = :tier")
        params["tier"] = tier

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    sql = text(f"""
        SELECT
            seller_id,
            seller_tier,
            seller_state,
            seller_region,
            total_orders,
            total_revenue,
            avg_order_value,
            avg_dispatch_days,
            sla_breach_count,
            sla_compliant_count,
            sla_breach_rate,
            sla_compliance_rate,
            avg_dispatch_speed_score,
            express_count,
            fast_count,
            standard_count,
            slow_count,
            avg_freight_burden_rate,
            rank_by_revenue,
            rank_by_sla_compliance,
            rank_by_volume,
            rank_by_speed,
            overall_score,
            overall_rank
        FROM acip_dbt_marts.mart_seller_leaderboard
        {where_clause}
        ORDER BY {order_col} ASC NULLS LAST
        LIMIT :limit
    """).bindparams(**params)

    rows = db.execute(sql).fetchall()
    return [dict(r._mapping) for r in rows]


# ---------------------------------------------------------------------------
# Real-time - DynamoDB
# ---------------------------------------------------------------------------

def get_seller_sla_status() -> list:
    table = get_table("seller_table")
    return safe_scan(table, Limit=50)


def get_realtime_marketplace_metrics() -> list:
    table = get_table("metrics_table")
    return safe_scan(
        table,
        FilterExpression="begins_with(pk, :domain)",
        ExpressionAttributeValues={":domain": "marketplace"},
        Limit=50,
    )

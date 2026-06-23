# ====================================================================
# AWS Commerce Intelligence Platform - Marketplace Router
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/routers/marketplace.py
# Purpose: 8 marketplace endpoints (2 real-time, 6 analytical)
# ====================================================================

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from database.postgres import get_db
from services import marketplace_service

router = APIRouter()


# ---------------------------------------------------------------------------
# Real-time endpoints - DynamoDB
# ---------------------------------------------------------------------------

@router.get("/realtime/sla-status")
def seller_sla_status():
    """Current seller SLA status from DynamoDB."""
    return marketplace_service.get_seller_sla_status()


@router.get("/realtime/metrics")
def realtime_marketplace_metrics():
    """Real-time marketplace operational metrics from DynamoDB."""
    return marketplace_service.get_realtime_marketplace_metrics()


# ---------------------------------------------------------------------------
# Analytical endpoints - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

@router.get("/analytics/sla-breach-by-tier")
def sla_breach_by_tier(db: Session = Depends(get_db)):
    """SLA breach rate, dispatch count, and avg dispatch time by seller tier."""
    return marketplace_service.get_sla_breach_by_tier(db)


@router.get("/analytics/top-sellers")
def top_sellers(
    limit: int = Query(default=20, ge=1, le=100, description="Number of sellers to return"),
    db: Session = Depends(get_db),
):
    """Top sellers by gross revenue with SLA and dispatch performance."""
    return marketplace_service.get_top_sellers(db, limit=limit)


@router.get("/analytics/price-volatility")
def price_volatility(db: Session = Depends(get_db)):
    """Price change statistics by category including significant change count."""
    return marketplace_service.get_price_volatility(db)


@router.get("/analytics/freight-burden")
def freight_burden(db: Session = Depends(get_db)):
    """Freight burden distribution by seller tier and burden level."""
    return marketplace_service.get_freight_burden(db)


@router.get("/analytics/dispatch-speed")
def dispatch_speed(db: Session = Depends(get_db)):
    """Dispatch speed bucket distribution by seller tier."""
    return marketplace_service.get_dispatch_speed_distribution(db)


@router.get("/analytics/seller-leaderboard")
def seller_leaderboard(
    tier: Optional[str] = Query(
        default=None,
        description="Filter by seller tier: platinum, gold, standard, new"
    ),
    rank_by: str = Query(
        default="overall",
        description="Sort by: overall, revenue, sla_compliance, volume, speed"
    ),
    limit: int = Query(default=20, ge=1, le=100, description="Number of sellers to return"),
    db: Session = Depends(get_db),
):
    """
    Seller performance leaderboard from mart_seller_leaderboard.
    Returns sellers ranked by revenue, SLA compliance, order volume,
    and dispatch speed with composite overall rank.
    """
    return marketplace_service.get_seller_leaderboard(
        db,
        tier=tier,
        rank_by=rank_by,
        limit=limit,
    )

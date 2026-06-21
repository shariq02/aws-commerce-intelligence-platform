# ====================================================================
# AWS Commerce Intelligence Platform - Ecommerce Router
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/routers/ecommerce.py
# Purpose: 8 ecommerce endpoints (4 analytical, 4 real-time)
# ====================================================================

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List

from database.postgres import get_db
from services import ecommerce_service

router = APIRouter()


# ---------------------------------------------------------------------------
# Analytical endpoints - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

@router.get("/analytics/daily-volume")
def daily_order_volume(
    days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    db: Session = Depends(get_db),
):
    """Daily order volume and revenue for the last N days."""
    return ecommerce_service.get_daily_order_volume(db, days=days)


@router.get("/analytics/customer-segments")
def customer_segments(db: Session = Depends(get_db)):
    """Order count, revenue, and return rate by customer segment."""
    return ecommerce_service.get_customer_segments(db)


@router.get("/analytics/fulfilment")
def fulfilment_analysis(db: Session = Depends(get_db)):
    """Fulfilment time distribution and on-time rate by bucket."""
    return ecommerce_service.get_fulfilment_analysis(db)


@router.get("/analytics/regional")
def regional_orders(
    limit: int = Query(default=10, ge=1, le=50, description="Number of regions to return"),
    db: Session = Depends(get_db),
):
    """Order count and revenue by state region."""
    return ecommerce_service.get_regional_orders(db, limit=limit)


# ---------------------------------------------------------------------------
# Real-time endpoints - DynamoDB
# ---------------------------------------------------------------------------

@router.get("/realtime/metrics")
def realtime_metrics():
    """Real-time ecommerce operational metrics from DynamoDB."""
    return ecommerce_service.get_realtime_metrics(domain="ecommerce")


@router.get("/realtime/anomalies")
def anomaly_flags():
    """Active anomaly flags for ecommerce domain from DynamoDB."""
    return ecommerce_service.get_anomaly_flags(domain="ecommerce")


@router.get("/realtime/volume-spike")
def volume_spike():
    """Current volume spike detection status from DynamoDB."""
    items = ecommerce_service.get_anomaly_flags(domain="ecommerce")
    spikes = [i for i in items if "volume_spike" in str(i.get("anomaly_type", "")).lower()]
    return spikes


@router.get("/realtime/cross-domain-metrics")
def cross_domain_realtime():
    """Real-time cross-domain metrics including ecommerce from DynamoDB."""
    return ecommerce_service.get_realtime_metrics(domain="cross_domain")

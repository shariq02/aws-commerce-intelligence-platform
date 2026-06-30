# ====================================================================
# AWS Commerce Intelligence Platform - Platform Router
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/routers/platform.py
# Purpose: 7 platform-wide endpoints (2 real-time, 5 analytical)
# ====================================================================

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from database.postgres import get_db
from services import platform_service

router = APIRouter()


# ---------------------------------------------------------------------------
# Real-time endpoints - DynamoDB
# ---------------------------------------------------------------------------

@router.get("/realtime/cross-domain-metrics")
def cross_domain_realtime():
    """Real-time cross-domain aggregated metrics from DynamoDB."""
    return platform_service.get_cross_domain_realtime()


@router.get("/realtime/all-anomalies")
def all_anomalies():
    """All active anomaly flags across all domains from DynamoDB."""
    return platform_service.get_all_anomalies()


# ---------------------------------------------------------------------------
# Analytical endpoints - PostgreSQL
# ---------------------------------------------------------------------------

@router.get("/analytics/cross-domain-summary")
def cross_domain_summary(
    months: int = Query(default=12, ge=1, le=36, description="Number of months to return"),
    db: Session = Depends(get_db),
):
    """Monthly cross-domain summary combining ecommerce, pharmacy, marketplace."""
    return platform_service.get_cross_domain_summary(db, months=months)


@router.get("/analytics/pipeline-watermarks")
def pipeline_watermarks(db: Session = Depends(get_db)):
    """Pipeline component watermarks and last run status from acip_quality."""
    return platform_service.get_pipeline_watermarks(db)


@router.get("/analytics/daily-domain-metrics")
def daily_domain_metrics(
    days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    db: Session = Depends(get_db),
):
    """Daily event count and value per domain from acip_gold."""
    return platform_service.get_daily_domain_metrics(db, days=days)


@router.get("/analytics/anomaly-rates")
def anomaly_rates(
    domain: Optional[str] = Query(
        default=None,
        description="Filter by domain: ecommerce, pharmacy, marketplace"
    ),
    spikes_only: bool = Query(
        default=False,
        description="Return only rows flagged as spike or drop"
    ),
    db: Session = Depends(get_db),
):
    """
    Daily domain event counts with rolling 30-day mean, standard deviation,
    and spike/drop flags from mart_domain_anomaly_rates.
    Serves use cases CD-02 (cross-domain anomaly comparison) and
    AD-01 (order volume spike detection, batch reframe).
    """
    return platform_service.get_anomaly_rates(db, domain=domain, spikes_only=spikes_only)


@router.get("/analytics/hourly-volume")
def hourly_volume(
    domain: Optional[str] = Query(
        default=None,
        description="Filter by domain: ecommerce, pharmacy, marketplace"
    ),
    db: Session = Depends(get_db),
):
    """
    Transaction count per hour of day (0-23) per domain from
    mart_hourly_transaction_volume. Batch approximation of real-time
    hourly tumbling window aggregation. Serves use case CD-04.
    """
    return platform_service.get_hourly_volume(db, domain=domain)

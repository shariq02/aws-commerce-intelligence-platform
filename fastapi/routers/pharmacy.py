# ====================================================================
# AWS Commerce Intelligence Platform - Pharmacy Router
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/routers/pharmacy.py
# Purpose: 6 pharmacy endpoints (2 real-time, 4 analytical)
# ====================================================================

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from database.postgres import get_db
from services import pharmacy_service

router = APIRouter()


# ---------------------------------------------------------------------------
# Real-time endpoints - DynamoDB
# ---------------------------------------------------------------------------

@router.get("/realtime/inventory-alerts")
def inventory_alerts():
    """Active inventory alerts from DynamoDB (critical and high levels)."""
    return pharmacy_service.get_inventory_alerts()


@router.get("/realtime/metrics")
def realtime_pharmacy_metrics():
    """Real-time pharmacy operational metrics from DynamoDB."""
    return pharmacy_service.get_realtime_pharmacy_metrics()


# ---------------------------------------------------------------------------
# Analytical endpoints - PostgreSQL acip_dbt_marts
# ---------------------------------------------------------------------------

@router.get("/analytics/drug-categories")
def drug_category_demand(db: Session = Depends(get_db)):
    """Total quantity and fill time by drug category and class."""
    return pharmacy_service.get_drug_category_demand(db)


@router.get("/analytics/stock-alerts")
def stock_alerts(
    level: Optional[str] = Query(
        default=None,
        description="Filter by alert level: critical, high, medium, normal"
    ),
    db: Session = Depends(get_db),
):
    """Products currently below reorder threshold requiring action."""
    return pharmacy_service.get_stock_alerts(db, level=level)


@router.get("/analytics/rx-otc-split")
def rx_otc_summary(db: Session = Depends(get_db)):
    """Prescription vs OTC volume, fill time, and event count comparison."""
    return pharmacy_service.get_rx_otc_summary(db)


@router.get("/analytics/peak-hours")
def peak_hour_analysis(db: Session = Depends(get_db)):
    """Dispensing event count and fill time by hour of day."""
    return pharmacy_service.get_peak_hour_analysis(db)

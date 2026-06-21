# ====================================================================
# AWS Commerce Intelligence Platform - Marketplace Response Models
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/models/marketplace.py
# Purpose: Pydantic models for marketplace endpoint responses
# ====================================================================

from pydantic import BaseModel
from typing import Optional


class SlaBreachItem(BaseModel):
    seller_tier: Optional[str]
    total_dispatches: int
    breached_count: int
    breach_rate: Optional[float]
    avg_dispatch_days: Optional[float]


class SellerPerformanceItem(BaseModel):
    seller_id: Optional[str]
    seller_tier: Optional[str]
    total_dispatches: int
    breach_rate: Optional[float]
    avg_dispatch_days: Optional[float]
    gross_revenue: Optional[float]
    net_revenue: Optional[float]


class PriceVolatilityItem(BaseModel):
    category: Optional[str]
    avg_change_pct: Optional[float]
    max_change_pct: Optional[float]
    price_change_events: int
    significant_changes: int


class FreightBurdenItem(BaseModel):
    freight_burden: Optional[str]
    seller_tier: Optional[str]
    dispatch_count: int
    avg_freight_ratio: Optional[float]
    avg_dispatch_days: Optional[float]


class SellerSlaStatusItem(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    seller_id: Optional[str]
    seller_tier: Optional[str]
    sla_status: Optional[str]
    breach_count: Optional[str]
    updated_at: Optional[str]

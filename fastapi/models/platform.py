# ====================================================================
# AWS Commerce Intelligence Platform - Platform Response Models
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/models/platform.py
# Purpose: Pydantic models for platform endpoint responses
# ====================================================================

from pydantic import BaseModel
from typing import Optional
from datetime import date


class CrossDomainSummaryItem(BaseModel):
    year: Optional[int]
    month: Optional[int]
    ecommerce_transactions: Optional[int]
    ecommerce_net_revenue: Optional[float]
    ecommerce_avg_order_value: Optional[float]
    ecommerce_returns: Optional[int]
    pharmacy_dispensing_events: Optional[int]
    pharmacy_avg_fill_time: Optional[float]
    pharmacy_critical_stock_events: Optional[int]
    marketplace_dispatch_events: Optional[int]
    marketplace_gross_revenue: Optional[float]
    marketplace_avg_dispatch_days: Optional[float]
    marketplace_sla_breaches: Optional[int]


class PipelineWatermarkItem(BaseModel):
    component: Optional[str]
    last_run_at: Optional[str]
    status: Optional[str]
    rows_processed: Optional[int]


class DomainMetricItem(BaseModel):
    metric_date: Optional[date]
    domain: Optional[str]
    event_count: Optional[int]
    total_value: Optional[float]


class PlatformHealthItem(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    domain: Optional[str]
    metric_type: Optional[str]
    value: Optional[str]
    updated_at: Optional[str]

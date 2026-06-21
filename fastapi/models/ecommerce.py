# ====================================================================
# AWS Commerce Intelligence Platform - Ecommerce Response Models
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/models/ecommerce.py
# Purpose: Pydantic models for ecommerce endpoint responses
# ====================================================================

from pydantic import BaseModel
from typing import Optional
from datetime import date


class OrderVolumeItem(BaseModel):
    full_date: Optional[date]
    order_count: int
    total_revenue: Optional[float]
    avg_order_value: Optional[float]


class CustomerSegmentItem(BaseModel):
    customer_segment: Optional[str]
    order_count: int
    total_revenue: Optional[float]
    avg_order_value: Optional[float]
    return_rate: Optional[float]


class FulfilmentBucketItem(BaseModel):
    fulfilment_bucket: Optional[str]
    order_count: int
    avg_fulfilment_days: Optional[float]
    on_time_rate: Optional[float]


class RegionalOrderItem(BaseModel):
    state_region: Optional[str]
    state: Optional[str]
    order_count: int
    total_revenue: Optional[float]


class AnomalyItem(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    domain: Optional[str]
    anomaly_type: Optional[str]
    detected_at: Optional[str]
    severity: Optional[str]
    details: Optional[str]


class RealtimeMetricItem(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    domain: Optional[str]
    metric_type: Optional[str]
    value: Optional[str]
    updated_at: Optional[str]

# ====================================================================
# AWS Commerce Intelligence Platform - Pharmacy Response Models
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/models/pharmacy.py
# Purpose: Pydantic models for pharmacy endpoint responses
# ====================================================================

from pydantic import BaseModel
from typing import Optional


class DrugCategoryItem(BaseModel):
    category: Optional[str]
    drug_class: Optional[str]
    is_prescription: Optional[bool]
    total_quantity: Optional[float]
    avg_fill_time_mins: Optional[float]
    event_count: int


class StockAlertItem(BaseModel):
    snapshot_key: Optional[int]
    product_id: Optional[str]
    category: Optional[str]
    stock_level: Optional[int]
    reorder_threshold: Optional[int]
    stock_buffer: Optional[int]
    alert_level: Optional[str]
    requires_action: Optional[bool]


class InventoryAlertItem(BaseModel):
    pk: Optional[str]
    sk: Optional[str]
    domain: Optional[str]
    product_id: Optional[str]
    alert_level: Optional[str]
    stock_level: Optional[str]
    reorder_threshold: Optional[str]
    days_of_supply: Optional[str]
    updated_at: Optional[str]


class RxOtcSummaryItem(BaseModel):
    is_prescription: Optional[bool]
    label: Optional[str]
    total_quantity: Optional[float]
    avg_fill_time_mins: Optional[float]
    event_count: int

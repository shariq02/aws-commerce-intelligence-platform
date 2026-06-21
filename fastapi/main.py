# ====================================================================
# AWS Commerce Intelligence Platform - FastAPI Entry Point
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/main.py
# Purpose: Application entry point, mounts all domain routers
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import FASTAPI_CONFIG, get_logger
from routers import ecommerce, pharmacy, marketplace, platform

logger = get_logger("api.main")

app = FastAPI(
    title="AWS Commerce Intelligence Platform API",
    description=(
        "Multi-domain operational intelligence platform serving real-time "
        "and analytical data across ecommerce, pharmacy, and marketplace domains."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(ecommerce.router, prefix="/ecommerce", tags=["Ecommerce"])
app.include_router(pharmacy.router, prefix="/pharmacy", tags=["Pharmacy"])
app.include_router(marketplace.router, prefix="/marketplace", tags=["Marketplace"])
app.include_router(platform.router, prefix="/platform", tags=["Platform"])


@app.get("/", tags=["Health"])
def root():
    return {
        "platform": "AWS Commerce Intelligence Platform",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
def health():
    return {
        "status": "healthy",
        "domains": ["ecommerce", "pharmacy", "marketplace"],
        "layers": ["realtime", "analytical"],
    }

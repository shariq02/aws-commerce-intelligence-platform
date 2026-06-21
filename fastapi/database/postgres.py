# ====================================================================
# AWS Commerce Intelligence Platform - PostgreSQL Connection
# Author: Sharique Mohammad
# Date: June 2026
# ====================================================================
# FILE: fastapi/database/postgres.py
# Purpose: SQLAlchemy session factory for PostgreSQL
# ====================================================================

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

from config import get_database_url, get_logger

logger = get_logger("api.database.postgres")

engine = create_engine(
    get_database_url(),
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=300,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency - yields a database session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection() -> bool:
    """Test PostgreSQL connection on startup."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("PostgreSQL connection: OK")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return False

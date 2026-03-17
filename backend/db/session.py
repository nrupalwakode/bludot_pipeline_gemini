"""
Database engine and session factory.
Change DATABASE_URL in .env to switch from SQLite → PostgreSQL with zero code changes.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base

# Default: SQLite local file
# To use PostgreSQL: set DATABASE_URL=postgresql://user:pass@host:5432/bludot_pipeline
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./bludot_pipeline.db")

# SQLite needs check_same_thread=False for FastAPI's threading model
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args,
    echo=False,  # Set True to see SQL queries in terminal during dev
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Create all tables. Safe to call on every startup (no-op if tables exist)."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """FastAPI dependency — yields a DB session and ensures it's closed after."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

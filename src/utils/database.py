"""Database utility functions and base models."""

import logging
import os
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from src.utils.config import DATA_DIR, DATABASE_URL

# Set up logging
logger = logging.getLogger(__name__)

# Create a base class for SQLAlchemy models
Base = declarative_base()

# Ensure the data directory exists
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

# Use SQLite by default in Codespace environment
sqlite_path = os.path.join(DATA_DIR, "crypto_analysis.db")
sqlite_url = f"sqlite:///{sqlite_path}"

# Try to use PostgreSQL if requested, but fall back to SQLite
try:
    use_postgres = os.environ.get("USE_POSTGRES", "false").lower() == "true"
    if use_postgres:
        engine = create_engine(DATABASE_URL)
        logger.info(f"Database engine created for PostgreSQL at {DATABASE_URL}")
    else:
        raise ValueError("Using SQLite by default")
except Exception as e:
    logger.info(f"Using SQLite database at {sqlite_path}: {str(e)}")
    engine = create_engine(sqlite_url)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_session():
    """Get a database session with context management.
    
    Usage:
        with get_db_session() as session:
            # Use session here
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        session.close()


def init_db():
    """Initialize the database by creating all tables."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise
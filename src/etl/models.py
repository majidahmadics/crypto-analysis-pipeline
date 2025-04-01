"""Database models for the ETL pipeline."""

import pandas as pd

from datetime import date, datetime
from sqlalchemy import Column, Date, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from src.utils.database import Base


class CryptoOHLCV(Base):
    """Model for cryptocurrency Open-High-Low-Close-Volume data."""
    
    __tablename__ = "crypto_ohlcv"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    date = Column(Date, nullable=False)
    datetime = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    adj_close = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Ensure no duplicate entries for the same symbol and datetime
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_datetime'),
    )
    
    def __repr__(self):
        return f"<CryptoOHLCV(symbol='{self.symbol}', date='{self.date}', close={self.close})>"
    
    @classmethod
    def from_dataframe_row(cls, row):
        """Create a model instance from a DataFrame row."""
        return cls(
            symbol=row["symbol"],
            date=row.name.date() if hasattr(row.name, "date") else row.name,
            datetime=row.name if isinstance(row.name, datetime) else datetime.combine(row.name, datetime.min.time()),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            adj_close=float(row["adj close"]) if "adj close" in row else None,
            volume=float(row["volume"]) if "volume" in row and not pd.isna(row["volume"]) else None
        )
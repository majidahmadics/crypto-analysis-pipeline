"""Extractor for cryptocurrency OHLCV data from yfinance."""

import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import yfinance as yf

from src.etl.base_extractor import BaseExtractor
from src.utils.config import DEFAULT_CRYPTOCURRENCIES, DEFAULT_START_DATE


class CryptoOHLCVExtractor(BaseExtractor):
    """Extractor for cryptocurrency OHLCV (Open, High, Low, Close, Volume) data.
    
    This class extracts historical price and volume data for cryptocurrencies
    using the yfinance library.
    """
    
    def __init__(
        self,
        symbols: List[str] = DEFAULT_CRYPTOCURRENCIES,
        start_date: str = DEFAULT_START_DATE,
        end_date: Optional[str] = None,
        interval: str = "1d",
        name: str = "crypto_ohlcv"
    ):
        """Initialize the cryptocurrency OHLCV extractor.
        
        Args:
            symbols: List of cryptocurrency symbols to extract data for.
            start_date: Start date for historical data in YYYY-MM-DD format.
            end_date: End date for historical data in YYYY-MM-DD format.
                If None, defaults to current date.
            interval: Data interval (1d, 1h, etc.).
            name: Name for this extractor instance.
        """
        super().__init__(name=name)
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date or datetime.datetime.now().strftime("%Y-%m-%d")
        self.interval = interval
        self.logger.info(
            f"Initialized with {len(symbols)} symbols, "
            f"start_date={start_date}, end_date={end_date}, interval={interval}"
        )
    
    def extract(self, symbols: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Extract OHLCV data for the specified cryptocurrencies.
        
        Args:
            symbols: Optional override for the list of symbols to extract.
                If None, uses the symbols specified during initialization.
                
        Returns:
            A dictionary mapping cryptocurrency symbols to DataFrames containing
            their historical OHLCV data.
        """
        symbols = symbols or self.symbols
        self.logger.info(f"Extracting data for {len(symbols)} symbols")
        
        results = {}
        for symbol in symbols:
            try:
                self.logger.info(f"Downloading data for {symbol}")
                data = yf.download(
                    tickers=symbol,
                    start=self.start_date,
                    end=self.end_date,
                    interval=self.interval,
                    progress=False,
                    show_errors=False
                )

                # Rename columns to standardized format
                data.columns = [col.lower() for col in data.columns]
                
                # Add symbol as a column
                data["symbol"] = symbol
                
                # Convert index to datetime if it's not already
                if not isinstance(data.index, pd.DatetimeIndex):
                    data.index = pd.to_datetime(data.index)
                
                # Add a date column for easier querying
                data["date"] = data.index.date
                
                results[symbol] = data
                self.logger.info(f"Successfully downloaded {len(data)} rows for {symbol}")
            except Exception as e:
                self.logger.error(f"Error downloading data for {symbol}: {str(e)}")
        
        return results
    
    def validate(self, data: Dict[str, pd.DataFrame]) -> bool:
        """Validate the extracted cryptocurrency data.
        
        Args:
            data: Dictionary mapping symbols to DataFrames of OHLCV data.
            
        Returns:
            True if all data is valid, False otherwise.
        """
        if not data:
            self.logger.warning("No data was extracted")
            return False
        
        valid = True
        for symbol, df in data.items():
            # Check if DataFrame is empty
            if df.empty:
                self.logger.warning(f"No data for {symbol}")
                valid = False
                continue
                
            # Check if required columns are present
            required_columns = ["open", "high", "low", "close", "volume", "symbol"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.warning(f"Missing columns for {symbol}: {missing_columns}")
                valid = False
            
            # Check for missing values in critical columns
            critical_columns = ["open", "high", "low", "close"]
            for col in critical_columns:
                if col in df.columns and df[col].isnull().any():
                    self.logger.warning(f"Missing values in {col} column for {symbol}")
                    valid = False
        
        return valid
    
    def log_extraction_stats(self, data: Dict[str, pd.DataFrame]) -> None:
        """Log statistics about the extracted data.
        
        Args:
            data: Dictionary mapping symbols to DataFrames of OHLCV data.
        """
        super().log_extraction_stats(data)
        
        if not data:
            return
            
        total_rows = sum(len(df) for df in data.values())
        self.logger.info(f"Total rows extracted: {total_rows}")
        
        for symbol, df in data.items():
            if df.empty:
                continue
                
            date_range = f"{df.index.min().date()} to {df.index.max().date()}"
            self.logger.info(f"{symbol}: {len(df)} rows, date range: {date_range}")
            
            # Log min/max/avg prices
            if "close" in df.columns:
                stats = {
                    "min": df["close"].min(),
                    "max": df["close"].max(),
                    "avg": df["close"].mean()
                }
                self.logger.info(
                    f"{symbol} price stats: min=${stats['min']:.2f}, "
                    f"max=${stats['max']:.2f}, avg=${stats['avg']:.2f}"
                )
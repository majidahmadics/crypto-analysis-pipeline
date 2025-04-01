"""Simple ETL script to test extraction and loading of cryptocurrency data."""

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.etl.crypto_ohlcv_extractor import CryptoOHLCVExtractor
from src.etl.models import CryptoOHLCV
from src.utils.config import PROJECT_ROOT, RAW_DATA_DIR
from src.utils.database import get_db_session, init_db

# Set up logging
Path(f"{PROJECT_ROOT}/logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{PROJECT_ROOT}/logs/simple_etl.log", mode="a"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("simple_etl")


def extract_crypto_data(symbols=None, start_date=None, end_date=None):
    """Extract cryptocurrency OHLCV data."""
    logger.info("Starting extraction of cryptocurrency OHLCV data")
    
    extractor = CryptoOHLCVExtractor(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date
    )
    
    data = extractor.extract()
    
    if extractor.validate(data):
        logger.info("Data validation successful")
    else:
        logger.warning("Data validation issues found")
    
    extractor.log_extraction_stats(data)
    
    return data


def save_to_csv(data, output_dir=RAW_DATA_DIR):
    """Save extracted data to CSV files."""
    logger.info(f"Saving data to CSV files in {output_dir}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for symbol, df in data.items():
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol}, skipping CSV save")
            continue
            
        # Create a clean filename
        filename = f"{symbol.replace('-', '_')}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save to CSV
        df.to_csv(filepath)
        logger.info(f"Saved {len(df)} rows for {symbol} to {filepath}")


def save_to_database(data):
    """Save extracted data to database."""
    logger.info("Saving data to database")
    
    total_rows = 0
    new_entries = 0
    
    with get_db_session() as session:
        for symbol, df in data.items():
            if df.empty:
                logger.warning(f"Empty DataFrame for {symbol}, skipping database save")
                continue
                
            logger.info(f"Processing {len(df)} rows for {symbol}")
            total_rows += len(df)
            
            # Process each row
            for index, row in df.iterrows():
                # Convert row to dictionary and add symbol
                row_dict = row.to_dict()
                row_dict["symbol"] = symbol
                
                # Check if entry already exists
                existing = session.query(CryptoOHLCV).filter(
                    CryptoOHLCV.symbol == symbol,
                    CryptoOHLCV.datetime == index
                ).first()
                
                if not existing:
                    # Create new entry
                    try:
                        entry = CryptoOHLCV.from_dataframe_row(row)
                        session.add(entry)
                        new_entries += 1
                    except Exception as e:
                        logger.error(f"Error creating entry for {symbol} at {index}: {str(e)}")
    
    logger.info(f"Database save complete. Total rows: {total_rows}, New entries: {new_entries}")


def run_simple_etl(symbols=None, start_date=None, end_date=None):
    """Run a simple ETL process for testing."""
    logger.info("Starting simple ETL process")
    
    # Initialize database
    try:
        init_db()
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        return
    
    # Extract data
    try:
        data = extract_crypto_data(symbols, start_date, end_date)
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return
    
    # Save to CSV
    try:
        save_to_csv(data)
    except Exception as e:
        logger.error(f"CSV save failed: {str(e)}")
    
    # Save to database
    try:
        save_to_database(data)
    except Exception as e:
        logger.error(f"Database save failed: {str(e)}")
    
    logger.info("Simple ETL process completed")


if __name__ == "__main__":
    # Test the ETL process with a limited dataset
    test_symbols = ["BTC-USD", "ETH-USD"]
    test_start_date = "2023-01-01"
    test_end_date = datetime.now().strftime("%Y-%m-%d")
    
    run_simple_etl(test_symbols, test_start_date, test_end_date)
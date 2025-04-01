"""Main entry point for the Crypto Analysis Pipeline."""

import argparse
import logging
from datetime import datetime
from pathlib import Path

from src.etl.simple_etl import run_simple_etl
from src.utils.config import DEFAULT_CRYPTOCURRENCIES, DEFAULT_START_DATE, PROJECT_ROOT

# Set up logging directory
Path(f"{PROJECT_ROOT}/logs").mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{PROJECT_ROOT}/logs/main.log", mode="a"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Cryptocurrency Analysis Pipeline")
    
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_CRYPTOCURRENCIES,
        help="List of cryptocurrency symbols to analyze"
    )
    
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Start date for historical data (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for historical data (YYYY-MM-DD)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the application."""
    logger.info("Starting Cryptocurrency Analysis Pipeline")
    
    args = parse_args()
    logger.info(f"Running with symbols={args.symbols}, start_date={args.start_date}, end_date={args.end_date}")
    
    # Run simple ETL process
    run_simple_etl(args.symbols, args.start_date, args.end_date)
    
    logger.info("Pipeline execution completed")


if __name__ == "__main__":
    main()
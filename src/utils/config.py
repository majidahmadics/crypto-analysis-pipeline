"""Configuration management module for the Crypto Analysis Pipeline."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Project base directories
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
FEATURES_DIR = DATA_DIR / "features"
CONFIG_DIR = PROJECT_ROOT / "config"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
FEATURES_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_USERNAME = os.getenv("DB_USERNAME", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "crypto_analysis")
DATABASE_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# API Keys
CRYPTO_NEWS_API_KEY = os.getenv("CRYPTO_NEWS_API_KEY", "")

# Default cryptocurrencies to track
DEFAULT_CRYPTOCURRENCIES = [
    "BTC-USD",
    "ETH-USD",
    "XRP-USD",
    "LTC-USD",
    "ADA-USD",
    "DOT-USD",
    "SOL-USD",
    "DOGE-USD",
]

# Time period for historical data
DEFAULT_START_DATE = "2018-01-01"

# ML model parameters
PREDICTION_HORIZON = 7  # days
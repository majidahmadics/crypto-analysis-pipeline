"""Tests for the CryptoOHLCVExtractor."""

import unittest
from datetime import datetime, timedelta

import pandas as pd

from src.etl.crypto_ohlcv_extractor import CryptoOHLCVExtractor


class TestCryptoOHLCVExtractor(unittest.TestCase):
    """Test cases for CryptoOHLCVExtractor."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Use a short date range to avoid long test times
        self.end_date = datetime.now().strftime("%Y-%m-%d")
        self.start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        self.symbols = ["BTC-USD", "ETH-USD"]
        
        self.extractor = CryptoOHLCVExtractor(
            symbols=self.symbols,
            start_date=self.start_date,
            end_date=self.end_date
        )
    
    def test_extraction(self):
        """Test that data can be extracted."""
        data = self.extractor.extract()
        
        # Check if we got data for each symbol
        self.assertEqual(len(data), len(self.symbols))
        
        for symbol in self.symbols:
            self.assertIn(symbol, data)
            
            df = data[symbol]
            # Check that the DataFrame is not empty
            self.assertFalse(df.empty)
            
            # Check for required columns
            for col in ["open", "high", "low", "close", "volume", "symbol"]:
                self.assertIn(col, df.columns)
            
            # Check that the symbol column has the correct value
            self.assertTrue(all(df["symbol"] == symbol))
    
    def test_validation(self):
        """Test data validation."""
        # Extract real data
        valid_data = self.extractor.extract()
        
        # Validation should pass for good data
        self.assertTrue(self.extractor.validate(valid_data))
        
        # Create invalid data (empty DataFrame)
        invalid_data = {
            "BTC-USD": pd.DataFrame(),
            "ETH-USD": valid_data["ETH-USD"] if "ETH-USD" in valid_data else pd.DataFrame()
        }
        
        # Validation should fail for invalid data
        self.assertFalse(self.extractor.validate(invalid_data))


if __name__ == "__main__":
    unittest.main()
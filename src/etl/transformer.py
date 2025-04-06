import os
import json
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator

class CryptoTransformer:
    """
    This class Transofrm data using TA library.
    """
    def __init__(self):
        """
        Initialize path of raw data and destination data
        """
        self.raw_data_dir = os.path.join(os.getcwd(), 'data', 'raw')
        self.processed_data_dir = os.path.join(os.getcwd(), 'data', 'processed')

    def load_raw_data(self, ohlcv_path: str = 'BTC-USD', news_path: str = 'BTC-USD'):
        """
        Load OHLCV and News data from the raw data directory.
        """
        
        # Load OHLCV data
        ohlcv_file = os.path.join(self.raw_data_dir, f'{ohlcv_path}_ohlcv.csv')
        self.ohlcv = pd.read_csv(ohlcv_file)
        self.ohlcv['Date'] = pd.to_datetime(self.ohlcv['Date']).dt.strftime('%Y-%m-%d')

        # Load News data
        news_file = os.path.join(self.raw_data_dir, f'{news_path}_news.json')
        with open(news_file, 'r') as file:
            data = json.load(file)

        # Transform News data into a DataFrame
        news_df = {
            "Date": [article["publishedAt"][:10] for article in data["articles"]],  # Extract YYYY-MM-DD
            "Title": [article["title"] for article in data["articles"]],
            "Description": [article["description"] for article in data["articles"]],
        }
        self.news_df = pd.DataFrame(news_df)
        self.news_df['Date'] = pd.to_datetime(self.news_df['Date']).dt.strftime('%Y-%m-%d')

    def merge_data(self):
        """
        Merge OHLCV and News data on the 'Date' column.
        """
        self.news_ohlc = pd.merge(self.news_df, self.ohlcv, on="Date", how="inner")
        self.news_ohlc = self.news_ohlc.sort_values(by="Date")

    def add_technical_indicators(self):
        """
        Add technical indicators like RSI and SMA to the merged DataFrame.
        """

        # Calculate RSI (Relative Strength Index)
        self.news_ohlc['RSI'] = RSIIndicator(close=self.news_ohlc['Close'], window=14).rsi()

        # Calculate SMA (Simple Moving Average)
        self.news_ohlc['SMA_Short'] = SMAIndicator(close=self.news_ohlc['Close'], window=10).sma_indicator()
        self.news_ohlc['SMA_Medium'] = SMAIndicator(close=self.news_ohlc['Close'], window=50).sma_indicator()
        self.news_ohlc['SMA_Long'] = SMAIndicator(close=self.news_ohlc['Close'], window=200).sma_indicator()

    def save_processed_data(self, processed_path: str = 'BTC-USD'):
        """
        Save the processed DataFrame to the processed data directory.
        """
        processed_file = os.path.join(self.processed_data_dir, f'{processed_path}_transformed.csv')
        self.news_ohlc.to_csv(processed_file, index=False)
        print(f"Processed data saved to {processed_file}")
import os
import yfinance as yf
from newsapi import NewsApiClient
import pandas as pd


class CryptoExtractor:
    """
    A class to extract OHLCV data using yfinance and news using newsapi.
    """
    def __init__(self, ticker: str, newsapi_key_env: str = "NEWSAPI_KEY"):
        """
        Initialize with a crypto ticker and environment variable for NewsAPI key.
        """
        self.ticker = ticker
        self.newsapi_key = os.getenv(newsapi_key_env)
        if not self.newsapi_key:
            raise ValueError("NewsAPI key not found in environment variables.")
        self.newsapi = NewsApiClient(api_key=self.newsapi_key)

    def extract_ohlcv(self, period="1y", interval="1d"):
        """Extract OHLCV data using yfinance."""
        data = yf.download(self.ticker, period=period, interval=interval)
        data.reset_index(inplace=True)
        return data

    def extract_news(self, query: str, from_date: str, to_date: str, language="en"):
        """Extract news articles using newsapi."""
        return self.newsapi.get_everything(q=query, from_param=from_date, to=to_date, language=language)

    def save_raw_data(self, ohlcv_data, news_data, directory="/workspaces/crypto-analysis-pipeline/data/raw"):
        """Save OHLCV and news data to the specified directory."""
        import os
        os.makedirs(directory, exist_ok=True)
        ohlcv_data.to_csv(f"{directory}/{self.ticker}_ohlcv.csv", index=False)
        with open(f"{directory}/{self.ticker}_news.json", "w") as f:
            import json
            json.dump(news_data, f)
            print(f"Data extracted succefully in {directory}")
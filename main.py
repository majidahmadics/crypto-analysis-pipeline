from src.etl.extractor import CryptoExtractor
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":
    ticker_query = {"BTC-USD": "Bitcoin", "ETH-USD": "Ethereum", "BNB-USD": "Binance"}
    for tick, quer in ticker_query.items():
        extractor = CryptoExtractor(ticker=tick)
        ohlcv = extractor.extract_ohlcv()
        news = extractor.extract_news(query=quer, from_date=(datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), to_date=datetime.today().strftime("%Y-%m-%d"))
        
        extractor.save_raw_data(ohlcv, news)
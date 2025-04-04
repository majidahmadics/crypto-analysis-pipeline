from src.etl.extractor import CryptoExtractor
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":
    extractor = CryptoExtractor(ticker="BTC-USD")
    ohlcv = extractor.extract_ohlcv()
    news = extractor.extract_news(query="Bitcoin", from_date=(datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), to_date=datetime.today().strftime("%Y-%m-%d"))
    extractor.save_raw_data(ohlcv, news)
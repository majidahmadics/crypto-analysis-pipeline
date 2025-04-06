from src.etl.extractor import CryptoExtractor
from src.etl.transformer import CryptoTransformer
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":
    # Extractor Pipeline
    assets = {"BTC/USD": "Bitcoin", "ETH/USD": "Ethereum", "DOGE/USD": "Dogecoin"}

    for symb, quer in assets.items():
        extractor = CryptoExtractor(symbol=symb)
        ohlcv = extractor.extract_ohlcv()
        news = extractor.extract_news(query=quer, from_date=(datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), to_date=datetime.today().strftime("%Y-%m-%d"))
        extractor.save_raw_data(ohlcv, news)

    
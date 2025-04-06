from src.etl.extractor import CryptoExtractor
from src.etl.transformer import CryptoTransformer
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":

    assets = {"BTC/USD": "Bitcoin", "ETH/USD": "Ethereum", "DOGE/USD": "Dogecoin"}

    for symb, quer in assets.items():
        # Extractor Pipeline
        extractor = CryptoExtractor(symbol=symb)
        ohlcv = extractor.extract_ohlcv()
        news = extractor.extract_news(query=quer, from_date=(datetime.today() - relativedelta(months=1)).strftime("%Y-%m-%d"), to_date=datetime.today().strftime("%Y-%m-%d"))
        extractor.save_raw_data(ohlcv, news)

        # transformer pipeline
        transformer = CryptoTransformer()
        transformer.load_raw_data(ohlcv_path=symb, news_path=symb)
        transformer.merge_data()
        transformer.add_technical_indicators()
        transformer.save_processed_data(processed_path=symb)

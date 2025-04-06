from src.etl.transformer import CryptoTransformer
from src.pipeline.step001_extractor_pipeline import extractor_pipeline


if __name__ == "__main__":

    assets = {"BTC/USD": "Bitcoin", "ETH/USD": "Ethereum", "DOGE/USD": "Dogecoin"}

    for symb, quer in assets.items():
        # Extractor Pipeline
        extractor_pipeline(symb, quer)

        # transformer pipeline
        transformer = CryptoTransformer()
        transformer.load_raw_data(ohlcv_path=symb, news_path=symb)
        transformer.merge_data()
        transformer.add_technical_indicators()
        transformer.save_processed_data(processed_path=symb)

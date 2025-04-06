from src.pipeline.step001_extractor_pipeline import extractor_pipeline
from src.pipeline.step002_transformer_pipeline import transformer_pipeline


if __name__ == "__main__":

    assets = {"BTC/USD": "Bitcoin", "ETH/USD": "Ethereum", "DOGE/USD": "Dogecoin"}

    for symb, quer in assets.items():
        # Extractor Pipeline
        extractor_pipeline(symb, quer)

        # transformer pipeline
        transformer_pipeline(symb)
        

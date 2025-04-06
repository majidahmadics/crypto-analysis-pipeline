from src.etl.transformer import CryptoTransformer

def transformer_pipeline(symb):
    transformer = CryptoTransformer()
    transformer.load_raw_data(ohlcv_path=symb, news_path=symb)
    transformer.merge_data()
    transformer.add_technical_indicators()
    transformer.save_processed_data(processed_path=symb)
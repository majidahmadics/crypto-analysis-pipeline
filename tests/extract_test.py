import eventlet
import yfinance as yf

eventlet.monkey_patch()


data = yf.download(tickers='BTC-USD', period='1my', interval='1d', auto_adjust=False)

print(data.head())
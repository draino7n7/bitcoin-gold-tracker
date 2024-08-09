import yfinance as yf

def debug_ticker(ticker_symbol):
    try:
        # Fetch the ticker
        ticker = yf.Ticker(ticker_symbol)
        
        # Fetch the current market price using the 'info' dictionary
        info = ticker.info
        if 'regularMarketPrice' in info:
            print(f"Current price of {ticker_symbol}: {info['regularMarketPrice']}")
        else:
            print(f"'regularMarketPrice' not found in the 'info' dictionary for {ticker_symbol}.")
            print("Available keys in the 'info' dictionary:")
            print(info.keys())
        
        # Fetch the latest historical data with a small period and interval
        hist_data = ticker.history(period="1d", interval="1m")
        if not hist_data.empty:
            print(f"Latest price from historical data of {ticker_symbol}: {hist_data['Close'].iloc[-1]}")
        else:
            print(f"No historical data available for {ticker_symbol}.")

    except Exception as e:
        print(f"Error occurred: {e}")

# Debugging Bitcoin price
debug_ticker("BTC-USD")

# Debugging Gold price
debug_ticker("GC=F")

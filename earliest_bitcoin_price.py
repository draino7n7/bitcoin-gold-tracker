import yfinance as yf

def get_earliest_bitcoin_price():
    # Create a Ticker object for Bitcoin
    btc = yf.Ticker("BTC-USD")
    
    # Fetch the historical daily price data with the maximum available period
    history = btc.history(period="max", interval="1d")
    
    # Check if any data was returned
    if history.empty:
        print("No historical data available for Bitcoin.")
        return
    
    # Find the earliest date with available price data
    earliest_date = history.index.min()
    earliest_price = history.loc[earliest_date]
    
    print(f"Earliest date with Bitcoin price: {earliest_date}")
    print(f"Open: {earliest_price['Open']}")
    print(f"High: {earliest_price['High']}")
    print(f"Low: {earliest_price['Low']}")
    print(f"Close: {earliest_price['Close']}")
    print(f"Volume: {earliest_price['Volume']}")

# Run the function to find the earliest Bitcoin price
get_earliest_bitcoin_price()

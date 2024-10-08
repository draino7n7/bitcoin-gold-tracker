import os
import time
import yfinance as yf
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import schedule

# Load environment variables from .env file
load_dotenv()

# Retrieve sensitive data from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

# Connect to your database
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Function to insert price into the database
def insert_price(timestamp, bitcoin_price, gold_price):
    cursor.execute(
        "INSERT INTO bitcoin_prices (timestamp, price) VALUES (%s, %s)", 
        (timestamp, float(bitcoin_price))
    )
    cursor.execute(
        "INSERT INTO gold_prices (timestamp, price) VALUES (%s, %s)", 
        (timestamp, float(gold_price))
    )
    conn.commit()

# Function to delete old data (older than 25 hours)
def delete_old_data():
    cursor.execute(
        "DELETE FROM bitcoin_prices WHERE timestamp < NOW() - INTERVAL '25 hours'"
    )
    cursor.execute(
        "DELETE FROM gold_prices WHERE timestamp < NOW() - INTERVAL '25 hours'"
    )
    conn.commit()

# Function to fetch Bitcoin and Gold prices every 50 seconds
def fetch_current_prices():
    try:
        # Get the current price of Bitcoin
        btc = yf.Ticker("BTC-USD")
        btc_data = btc.history(period="1d", interval="1m")
        bitcoin_price = btc_data['Close'].iloc[-1]

        # Get the current price of Gold
        gold = yf.Ticker("GC=F")  # "GC=F" is the Yahoo Finance ticker symbol for Gold futures
        gold_data = gold.history(period="1d", interval="1m")
        gold_price = gold_data['Close'].iloc[-1]

        # Get the current UTC timestamp using timezone-aware datetime
        current_time = datetime.now(timezone.utc)

        # Insert the prices into the database with the same timestamp
        insert_price(current_time, bitcoin_price, gold_price)

        # Delete data older than 25 hours
        delete_old_data()

        print(f"Fetched and inserted current prices at {current_time}")

    except Exception as e:
        print(f"Error occurred: {e}")

# Function to get the earliest and latest dates in the database for a given table
def get_date_range(table_name):
    cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}")
    return cursor.fetchone()

# Function to fetch historical data from Yahoo Finance
def fetch_historical_data(ticker_symbol, start_date, end_date):
    ticker = yf.Ticker(ticker_symbol)
    history = ticker.history(start=start_date, end=end_date, interval="1d")
    return history

# Function to insert data into the historical tables
def insert_data(table_name, data):
    for date, row in data.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {table_name} (date, open_price, close_price, high_price, low_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """,
            (
                date, 
                float(row['Open']), 
                float(row['Close']), 
                float(row['High']), 
                float(row['Low'])
            )
        )
    conn.commit()

# Function to update historical Bitcoin data
def update_bitcoin_data():
    print("Updating Bitcoin data...")
    table_name = "bitcoin_price_history"
    start_date, end_date = get_date_range(table_name)
    min_date = datetime(2015, 1, 1)

    if start_date is None or end_date is None:
        start_date = min_date
    else:
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = max(end_date + timedelta(days=1), min_date)

    end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date.strftime('%Y-%m-%d') < end_date:
        data = fetch_historical_data("BTC-USD", start_date.strftime('%Y-%m-%d'), end_date)
        insert_data(table_name, data)
        print("Bitcoin historical data updated.")

# Function to update historical Gold data
def update_gold_data():
    print("Updating Gold data...")
    table_name = "gold_price_history"
    start_date, end_date = get_date_range(table_name)
    min_date = datetime(2015, 1, 1)

    if start_date is None or end_date is None:
        start_date = min_date
    else:
        end_date = datetime.combine(end_date, datetime.min.time())
        start_date = max(end_date + timedelta(days=1), min_date)

    end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date.strftime('%Y-%m-%d') < end_date:
        data = fetch_historical_data("GC=F", start_date.strftime('%Y-%m-%d'), end_date)
        insert_data(table_name, data)
        print("Gold historical data updated.")

# Function to update both Bitcoin and Gold historical data
def update_historical_data():
    update_bitcoin_data()
    update_gold_data()

# Schedule the historical data update twice a day
schedule.every().day.at("00:00").do(update_historical_data)
schedule.every().day.at("12:00").do(update_historical_data)

# Run the data update process
if __name__ == "__main__":
    # Fetch current prices every 50 seconds
    while True:
        fetch_current_prices()
        schedule.run_pending()
        time.sleep(50)

    # Close the connection
    cursor.close()
    conn.close()

    print("Data update complete.")

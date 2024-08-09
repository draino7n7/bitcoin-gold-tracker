import os
import time
import yfinance as yf
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

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

# Fetch Bitcoin and Gold prices every second
while True:
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

        # Wait for 1 minute before the next fetch
        time.sleep(60)
    
    except Exception as e:
        print(f"Error occurred: {e}")
        time.sleep(60)  # Wait 60 seconds before retrying

# Close the connection (you can implement a proper shutdown process)
cursor.close()
conn.close()

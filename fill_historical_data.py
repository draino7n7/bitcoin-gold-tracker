import os
import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve sensitive data from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

# Connect to your PostgreSQL database
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

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

# Check and fill missing data for Bitcoin
def update_bitcoin_data():
    print("Updating Bitcoin data...")
    table_name = "bitcoin_price_history"
    start_date, end_date = get_date_range(table_name)
    min_date = datetime(2015, 1, 1)

    if start_date is None or end_date is None:
        # If the table is empty, start from the specified minimum date
        start_date = min_date
    else:
        # Ensure end_date is a datetime object before adding timedelta
        end_date = datetime.combine(end_date, datetime.min.time())
        # Start from the day after the latest available date or from the minimum date, whichever is later
        start_date = max(end_date + timedelta(days=1), min_date)

    end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date.strftime('%Y-%m-%d') < end_date:
        data = fetch_historical_data("BTC-USD", start_date.strftime('%Y-%m-%d'), end_date)
        insert_data(table_name, data)

# Check and fill missing data for Gold
def update_gold_data():
    print("Updating Gold data...")
    table_name = "gold_price_history"
    start_date, end_date = get_date_range(table_name)
    min_date = datetime(2015, 1, 1)

    if start_date is None or end_date is None:
        # If the table is empty, start from the specified minimum date
        start_date = min_date
    else:
        # Ensure end_date is a datetime object before adding timedelta
        end_date = datetime.combine(end_date, datetime.min.time())
        # Start from the day after the latest available date or from the minimum date, whichever is later
        start_date = max(end_date + timedelta(days=1), min_date)

    end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date.strftime('%Y-%m-%d') < end_date:
        data = fetch_historical_data("GC=F", start_date.strftime('%Y-%m-%d'), end_date)
        insert_data(table_name, data)

# Main function to update both Bitcoin and Gold data
def update_data():
    update_bitcoin_data()
    update_gold_data()

# Run the data update process
if __name__ == "__main__":
    update_data()

    # Close the connection
    cursor.close()
    conn.close()

    print("Data update complete.")

import time
import requests
import json
from datetime import datetime
from confluent_kafka import Producer
import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Constants
API_BASE_URL = "https://mgo2gncdj3.execute-api.ap-south-1.amazonaws.com"
OPTION_CHAIN_ENDPOINT = f"{API_BASE_URL}/index-option-chain"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "option_chain_data"
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

# Establish a PostgreSQL connection
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Ensure the option_chain table exists
def create_option_chain_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS option_chain (
        strike_price NUMERIC NOT NULL,
        expiry_date DATE NOT NULL,
        option_type VARCHAR(2) NOT NULL,
        open_interest NUMERIC,
        change_in_open_interest NUMERIC,
        pchange_in_open_interest NUMERIC,
        total_traded_volume NUMERIC,
        implied_volatility NUMERIC,
        last_price NUMERIC,
        change NUMERIC,
        p_change NUMERIC,
        total_buy_quantity NUMERIC,
        total_sell_quantity NUMERIC,
        bid_qty NUMERIC,
        bid_price NUMERIC,
        ask_qty NUMERIC,
        ask_price NUMERIC,
        underlying_value NUMERIC,
        timestamp TIMESTAMP NOT NULL,
        PRIMARY KEY (strike_price, option_type, expiry_date, timestamp)
    );
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
            conn.commit()
            st.success("Database table 'option_chain' is ready.")
        except Exception as e:
            st.error(f"Error creating option_chain table: {e}")
        finally:
            conn.close()

# Initialize Kafka Producer
def get_kafka_producer():
    try:
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        return producer
    except Exception as e:
        st.error(f"Error initializing Kafka Producer: {e}")
        return None

# Fetch option chain data from the API
def fetch_option_chain(symbol):
    try:
        response = requests.get(OPTION_CHAIN_ENDPOINT, params={"symbol": symbol})
        response.raise_for_status()
        return response.json()["optionChainData"]["records"]["data"]
    except Exception as e:
        st.error(f"Error fetching option chain data: {e}")
        return None

# Stream option chain data to Kafka
def stream_option_data(producer):
    if producer is None:
        st.error("Kafka Producer is not initialized.")
        return

    symbol = "NIFTY"  # Replace with the desired index symbol

    # Ensure the table exists
    create_option_chain_table()

    while True:
        data = fetch_option_chain(symbol)
        if data:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for record in data:
                for option_type in ["CE", "PE"]:
                    if option_type in record:
                        option_data = record[option_type]
                        option_data["optionType"] = option_type
                        option_data["timestamp"] = timestamp
                        try:
                            producer.produce(KAFKA_TOPIC, value=json.dumps(option_data).encode("utf-8"))
                            producer.flush()
                            store_option_data_in_db(option_data)
                        except Exception as e:
                            st.error(f"Error streaming data to Kafka or storing in DB: {e}")
        time.sleep(60)  # Fetch data every 60 seconds

# Store or update option chain data in PostgreSQL
def store_option_data_in_db(option_data):
    required_keys = [
        "strikePrice", "expiryDate", "optionType", "openInterest", "changeinOpenInterest",
        "pchangeinOpenInterest", "totalTradedVolume", "impliedVolatility", "lastPrice",
        "change", "pChange", "totalBuyQuantity", "totalSellQuantity", "bidQty", "bidprice",
        "askQty", "askPrice", "underlyingValue", "timestamp"
    ]
    
    # Extract only the required keys and maintain the order
    try:
        values = {key: option_data.get(key, None) for key in required_keys}
        upsert_query = """
        INSERT INTO option_chain (
            strike_price, expiry_date, option_type, open_interest, change_in_open_interest,
            pchange_in_open_interest, total_traded_volume, implied_volatility, last_price,
            change, p_change, total_buy_quantity, total_sell_quantity, bid_qty, bid_price,
            ask_qty, ask_price, underlying_value, timestamp
        ) VALUES %s
        ON CONFLICT (strike_price, option_type, expiry_date, timestamp)
        DO UPDATE SET
            open_interest = EXCLUDED.open_interest,
            change_in_open_interest = EXCLUDED.change_in_open_interest,
            pchange_in_open_interest = EXCLUDED.pchange_in_open_interest,
            total_traded_volume = EXCLUDED.total_traded_volume,
            implied_volatility = EXCLUDED.implied_volatility,
            last_price = EXCLUDED.last_price,
            change = EXCLUDED.change,
            p_change = EXCLUDED.p_change,
            total_buy_quantity = EXCLUDED.total_buy_quantity,
            total_sell_quantity = EXCLUDED.total_sell_quantity,
            bid_qty = EXCLUDED.bid_qty,
            bid_price = EXCLUDED.bid_price,
            ask_qty = EXCLUDED.ask_qty,
            ask_price = EXCLUDED.ask_price,
            underlying_value = EXCLUDED.underlying_value;
        """
        
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    execute_values(cursor, upsert_query, [[values[key] for key in required_keys]])
                conn.commit()
            except Exception as e:
                st.error(f"Error inserting/updating data in PostgreSQL: {e}")
            finally:
                conn.close()
    except KeyError as e:
        st.error(f"Missing required key in option data: {e}")


# Display stored option chain data in Streamlit with a filter and sorting by latest timestamp, expiry_date, and ascending strike_price
def display_stored_data():
    st.title("📊 Stored Option Chain Data Viewer (Filtered by Total Traded Volume > 10000 and Sorted by Latest Timestamp, Expiry Date, and Ascending Strike Price)")
    create_option_chain_table()  # Ensure the table exists before querying
    conn = get_db_connection()
    if conn:
        try:
            query = """
                SELECT *
                FROM option_chain
                WHERE total_traded_volume > 10000
                ORDER BY timestamp DESC, strike_price ASC;
            """
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                st.dataframe(df)

                # Add a button to download the DataFrame as a CSV file
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download data as CSV",
                    data=csv,
                    file_name='option_chain_data.csv',
                    mime='text/csv',
                )
            else:
                st.info("No data found with total traded volume greater than 10000.")
        except Exception as e:
            st.error(f"Error fetching data from PostgreSQL: {e}")
        finally:
            conn.close()

# Main function to handle producer, consumer, and display
def main():
    st.sidebar.title("Mode Selection")
    mode = st.sidebar.radio("Choose Mode", ["Stream Option Data", "Display Filtered and Sorted Data"])

    if mode == "Stream Option Data":
        producer = get_kafka_producer()
        stream_option_data(producer)
    elif mode == "Display Filtered and Sorted Data":
        display_stored_data()

if __name__ == "__main__":
    main()

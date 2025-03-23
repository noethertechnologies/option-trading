import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime

# Constants
DB_CONFIG = {
    "dbname": "option_data",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

LOT_SIZE = 75  # Each option lot size

# Function to get PostgreSQL connection
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

# Function to fetch last_price data from PostgreSQL
def fetch_last_price_data(strike_price, expiry_date, option_type):
    query = """
        SELECT timestamp, last_price, open_interest, change_in_open_interest,total_traded_volume, implied_volatility, total_buy_quantity, total_sell_quantity, bid_qty, bid_price, ask_qty, ask_price
        FROM option_data
        WHERE strike_price = %s
          AND expiry_date = %s
          AND option_type = %s
        ORDER BY timestamp ASC;
    """
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=(strike_price, expiry_date, option_type))
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
        finally:
            conn.close()
    else:
        return None

# Function to calculate profit/loss based on order type and lots
def calculate_profit_loss(order_type, entry_price, num_lots, data):
    multiplier = LOT_SIZE * num_lots
    if order_type == "Buy":
        return (data["last_price"] - entry_price) * multiplier
    elif order_type == "Sell":
        return (entry_price - data["last_price"]) * multiplier

# Streamlit interface
def main():
    st.title("📈 Option Chain Viewer, Order Placement, and P/L Tracker")
    st.sidebar.title("Order Placement Options")

    # User input for filtering
    strike_price = st.sidebar.number_input("Enter Strike Price:", min_value=0.0, value=0.0, step=0.5)
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    option_type = st.sidebar.selectbox("Select Option Type (CE/PE):", ["CE", "PE"])
    order_type = st.sidebar.selectbox("Order Type (Buy/Sell):", ["Buy", "Sell"])
    entry_price = st.sidebar.number_input("Enter Entry Price:", min_value=0.0, value=0.0, step=0.01)
    num_lots = st.sidebar.number_input("Enter Number of Lots:", min_value=1, value=1, step=1)

    # Fetch and display data on button click
    if st.sidebar.button("Place Order and Track P/L"):
        with st.spinner("Fetching data..."):
            data = fetch_last_price_data(strike_price, expiry_date, option_type)
            if data is not None and not data.empty:
                st.success("Data fetched successfully!")
                data["timestamp"] = pd.to_datetime(data["timestamp"])

                # Calculate profit/loss
                data["profit_loss"] = calculate_profit_loss(order_type, entry_price, num_lots, data)

                # Display data in Streamlit
                st.subheader(f"Profit/Loss for {order_type} Order ({strike_price}, {expiry_date})")
                st.write(data[["timestamp", "last_price","open_interest","change_in_open_interest","total_traded_volume", "implied_volatility", "total_buy_quantity",  "total_sell_quantity", "bid_qty", "bid_price", "ask_qty", "ask_price",  "profit_loss"]])

                # Plot the profit/loss
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.plot(data["timestamp"], data["profit_loss"], label="Profit/Loss", marker="o", linestyle="-", color="green")
                ax.axhline(0, color="red", linestyle="--", linewidth=1)  # Mark breakeven line
                ax.set_xlabel("Timestamp")
                ax.set_ylabel("Profit/Loss")
                ax.set_title(f"Profit/Loss Over Time for {order_type} Order ({strike_price}, {expiry_date})")
                ax.legend()
                ax.grid(True)
                st.pyplot(fig)
            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

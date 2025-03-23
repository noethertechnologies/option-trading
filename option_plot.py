import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime

# Constants
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

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
        SELECT timestamp, last_price,  open_interest, change_in_open_interest,total_traded_volume, implied_volatility, total_buy_quantity, total_sell_quantity, bid_qty, bid_price, ask_qty, ask_price
        FROM option_chain
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

# Streamlit interface
def main():
    st.title("📈 Option Chain Data Viewer and Plotter")
    st.sidebar.title("Filter Options")
    
    # User input for filtering
    strike_price = st.sidebar.number_input("Enter Strike Price:", min_value=0.0, value=0.0, step=0.5)
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    option_type = st.sidebar.selectbox("Select Option Type (CE/PE):", ["CE", "PE"])
    
    # Fetch and display data on button click
    if st.sidebar.button("Fetch and Plot Data"):
        with st.spinner("Fetching data..."):
            data = fetch_last_price_data(strike_price, expiry_date, option_type)
            if data is not None and not data.empty:
                st.success("Data fetched successfully!")

                # Plot the data
                st.subheader("Last Price vs Timestamp")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.plot(pd.to_datetime(data["timestamp"]), data["last_price"], marker="o", linestyle="-")
                ax.set_xlabel("Timestamp")
                ax.set_ylabel("Last Price")
                ax.set_title(f"Last Price Trend for {option_type} ({strike_price}, {expiry_date})")
                ax.grid(True)
                st.pyplot(fig)
            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

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

# Function to fetch data from PostgreSQL
def fetch_option_data(strike_price, expiry_date, option_type):
    query = """
        SELECT timestamp, last_price, implied_volatility, total_traded_volume, open_interest
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
    st.title("📊 Option Chain Data Viewer and Plotter")
    st.sidebar.title("Filter Options")
    
    # User inputs for filtering
    strike_price = st.sidebar.number_input("Enter Strike Price:", min_value=0.0, value=0.0, step=0.5)
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    option_type = st.sidebar.selectbox("Select Option Type (CE/PE):", ["CE", "PE"])
    
    # Fetch and plot data
    if st.sidebar.button("Fetch and Plot Data"):
        with st.spinner("Fetching data..."):
            data = fetch_option_data(strike_price, expiry_date, option_type)
            if data is not None and not data.empty:
                st.success("Data fetched successfully!")

                # Plot Implied Volatility vs Last Price
                st.subheader(f"Implied Volatility vs Last Price for {option_type} ({strike_price}, {expiry_date})")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.scatter(data["last_price"], data["implied_volatility"], color="blue", alpha=0.7)
                ax.set_xlabel("Last Price")
                ax.set_ylabel("Implied Volatility")
                ax.set_title("Implied Volatility vs Last Price")
                ax.grid(True)
                st.pyplot(fig)

                # Plot Total Traded Volume vs Last Price
                st.subheader(f"Total Traded Volume vs Last Price for {option_type} ({strike_price}, {expiry_date})")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.scatter(data["last_price"], data["total_traded_volume"], color="green", alpha=0.7)
                ax.set_xlabel("Last Price")
                ax.set_ylabel("Total Traded Volume")
                ax.set_title("Total Traded Volume vs Last Price")
                ax.grid(True)
                st.pyplot(fig)

                # Plot Open Interest vs Last Price
                st.subheader(f"Open Interest vs Last Price for {option_type} ({strike_price}, {expiry_date})")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.scatter(data["last_price"], data["open_interest"], color="red", alpha=0.7)
                ax.set_xlabel("Last Price")
                ax.set_ylabel("Open Interest")
                ax.set_title("Open Interest vs Last Price")
                ax.grid(True)
                st.pyplot(fig)

                # Plot Time Series Data
                st.subheader(f"Time Series Trends for {option_type} ({strike_price}, {expiry_date})")
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.plot(pd.to_datetime(data["timestamp"]), data["last_price"], label="Last Price", marker="o")
                ax.plot(pd.to_datetime(data["timestamp"]), data["implied_volatility"], label="Implied Volatility", marker="x")
                ax.plot(pd.to_datetime(data["timestamp"]), data["total_traded_volume"], label="Total Traded Volume", marker="s")
                ax.plot(pd.to_datetime(data["timestamp"]), data["open_interest"], label="Open Interest", marker="d")
                ax.set_xlabel("Timestamp")
                ax.set_ylabel("Values")
                ax.legend()
                ax.set_title("Time Series Trends")
                ax.grid(True)
                st.pyplot(fig)

            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

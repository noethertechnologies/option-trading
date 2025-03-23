import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime
from math import log, sqrt
from scipy.stats import norm

# Constants and configuration
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

LOT_SIZE = 75  # Each option lot size

# Establish a connection to the PostgreSQL database
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

# Fetch option chain data based on user inputs
def fetch_option_chain_data(strike_price, expiry_date, option_type):
    query = """
        SELECT timestamp, strike_price, expiry_date, option_type, open_interest, change_in_open_interest,
               pchange_in_open_interest, total_traded_volume, implied_volatility, last_price, change, p_change,
               total_buy_quantity, total_sell_quantity, bid_qty, bid_price, ask_qty, ask_price, underlying_value
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
    return None

# Calculate Delta using the Black-Scholes model
def calculate_delta(option_type, S, K, T, r, sigma):
    """
    Args:
        option_type (str): "CE" for Call or "PE" for Put.
        S (float): Underlying asset price.
        K (float): Strike price.
        T (float): Time to expiration in years.
        r (float): Risk-free rate (e.g., 0.05 for 5%).
        sigma (float): Implied volatility (decimal form).
    
    Returns:
        float: Delta value.
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0

    try:
        d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
        if option_type == "CE":  # Call option delta
            return norm.cdf(d1)
        elif option_type == "PE":  # Put option delta
            return -norm.cdf(-d1)
        else:
            raise ValueError("Invalid option type. Use 'CE' for Call or 'PE' for Put.")
    except Exception as e:
        st.error(f"Error calculating Delta: {e}")
        return 0.0

# Generate a trading signal based on the delta value
def generate_trading_signal(delta, threshold=0.1):
    if abs(delta) > threshold:
        return "Strong Delta movement: Potential trend shift"
    return "No significant Delta movement"

# Main Streamlit interface
def main():
    st.title("📈 Option Chain Viewer with Delta Computation")
    st.sidebar.title("Option Filter")

    # Get user inputs
    strike_price = st.sidebar.number_input("Enter Strike Price:", min_value=0.0, value=0.0, step=0.5)
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    option_type = st.sidebar.selectbox("Select Option Type (CE/PE):", ["CE", "PE"])

    if st.sidebar.button("Fetch and Compute Delta"):
        with st.spinner("Fetching data..."):
            data = fetch_option_chain_data(strike_price, expiry_date, option_type)
            if data is not None and not data.empty:
                st.success("Data fetched successfully!")
                # Ensure timestamp is a datetime type and compute time to expiration
                data["timestamp"] = pd.to_datetime(data["timestamp"])
                expiry_date_ts = pd.Timestamp(expiry_date)
                data["time_to_expiry"] = (expiry_date_ts - data["timestamp"]).dt.days / 365.0

                # Compute delta for each record
                data["delta"] = data.apply(lambda row: calculate_delta(
                    row["option_type"],
                    row["underlying_value"],
                    row["strike_price"],
                    row["time_to_expiry"],
                    0.05,  # risk-free rate; adjust as necessary
                    row["implied_volatility"]
                ), axis=1)

                # Generate trading signals based on delta values
                data["trading_signal"] = data["delta"].apply(generate_trading_signal)

                # Display selected columns including delta and trading signals
                st.subheader(f"Option Chain Data with Delta for {option_type} (Strike: {strike_price}, Expiry: {expiry_date})")
                st.write(data[["timestamp", "strike_price", "last_price", "delta", "trading_signal"]])

                # Plot last price and delta trends
                st.subheader("Trend Plot")
                fig, ax1 = plt.subplots(figsize=(10, 5))

                ax1.plot(data["timestamp"], data["last_price"], marker="o", linestyle="-", color="blue", label="Last Price")
                ax1.set_xlabel("Timestamp")
                ax1.set_ylabel("Last Price", color="blue")
                ax1.tick_params(axis="y", labelcolor="blue")

                # Create a secondary y-axis to plot delta
                ax2 = ax1.twinx()
                ax2.plot(data["timestamp"], data["delta"], marker="x", linestyle="--", color="red", label="Delta")
                ax2.set_ylabel("Delta", color="red")
                ax2.tick_params(axis="y", labelcolor="red")

                plt.title(f"Last Price and Delta Trend for {option_type} (Strike: {strike_price}, Expiry: {expiry_date})")
                fig.tight_layout()
                st.pyplot(fig)
            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

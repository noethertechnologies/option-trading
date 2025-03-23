import streamlit as st
import pandas as pd
import psycopg2
from scipy.stats import norm
from math import log, sqrt, exp
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
DAYS_IN_YEAR = 365  # Used to normalize Theta calculation

# Black-Scholes Greeks Calculation Functions
def calculate_greeks(option_type, S, K, T, r, sigma):
    d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)

    if option_type == "CE":
        delta = norm.cdf(d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * sqrt(T)) 
                 - r * K * exp(-r * T) * norm.cdf(d2)) / DAYS_IN_YEAR
        rho = K * T * exp(-r * T) * norm.cdf(d2)
    elif option_type == "PE":
        delta = -norm.cdf(-d1)
        theta = (-S * norm.pdf(d1) * sigma / (2 * sqrt(T)) 
                 + r * K * exp(-r * T) * norm.cdf(-d2)) / DAYS_IN_YEAR
        rho = -K * T * exp(-r * T) * norm.cdf(-d2)

    gamma = norm.pdf(d1) / (S * sigma * sqrt(T))
    vega = S * norm.pdf(d1) * sqrt(T) / 100  # Vega is reported per 1% change in volatility

    return delta, gamma, vega, theta, rho

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
        SELECT timestamp, last_price
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

# Streamlit Interface
def main():
    st.title("📈 Option Chain Data Viewer and Greeks Plotter")
    st.sidebar.title("Filter Options")

    # User input for filtering
    strike_price = st.sidebar.number_input("Enter Strike Price:", min_value=0.0, value=0.0, step=0.5)
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    option_type = st.sidebar.selectbox("Select Option Type (CE/PE):", ["CE", "PE"])
    risk_free_rate = st.sidebar.number_input("Enter Risk-Free Rate (in %):", min_value=0.0, value=5.0, step=0.1) / 100
    volatility = st.sidebar.number_input("Enter Implied Volatility (in %):", min_value=0.0, value=20.0, step=0.1) / 100
    current_price = st.sidebar.number_input("Enter Current Underlying Price:", min_value=0.0, value=0.0, step=0.1)

    if st.sidebar.button("Fetch and Plot Data"):
        with st.spinner("Fetching data..."):
            data = fetch_last_price_data(strike_price, expiry_date, option_type)
            if data is not None and not data.empty:
                st.success("Data fetched successfully!")
                data["timestamp"] = pd.to_datetime(data["timestamp"])

                # Calculate Time to Expiration (T) in years
                T = (expiry_date - datetime.now().date()).days / DAYS_IN_YEAR
                if T <= 0:
                    st.warning("The expiry date must be in the future.")
                    return

                # Calculate Greeks
                greeks = data["last_price"].apply(
                    lambda last_price: calculate_greeks(
                        option_type, current_price, strike_price, T, risk_free_rate, volatility
                    )
                )
                data["Delta"], data["Gamma"], data["Vega"], data["Theta"], data["Rho"] = zip(*greeks)

                # Display data
                st.subheader("Option Data with Greeks")
                st.write(data)

                # Plot Last Price and Greeks
                fig, ax = plt.subplots(3, 1, figsize=(12, 15), sharex=True)

                # Plot Last Price
                ax[0].plot(data["timestamp"], data["last_price"], label="Last Price", color="blue", marker="o")
                ax[0].set_title("Last Price")
                ax[0].set_ylabel("Price")
                ax[0].legend()
                ax[0].grid()

                # Plot Delta, Gamma, and Vega
                ax[1].plot(data["timestamp"], data["Delta"], label="Delta", color="green", marker="o")
                ax[1].plot(data["timestamp"], data["Gamma"], label="Gamma", color="red", marker="o")
                ax[1].plot(data["timestamp"], data["Vega"], label="Vega", color="orange", marker="o")
                ax[1].set_title("Delta, Gamma, and Vega")
                ax[1].set_ylabel("Values")
                ax[1].legend()
                ax[1].grid()

                # Plot Theta and Rho
                ax[2].plot(data["timestamp"], data["Theta"], label="Theta", color="purple", marker="o")
                ax[2].plot(data["timestamp"], data["Rho"], label="Rho", color="brown", marker="o")
                ax[2].set_title("Theta and Rho")
                ax[2].set_ylabel("Values")
                ax[2].legend()
                ax[2].grid()

                plt.xlabel("Timestamp")
                st.pyplot(fig)
            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

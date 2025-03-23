import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Database Configuration
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

# Connect to PostgreSQL Database
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

# Fetch Option Data
def fetch_option_data(expiry_date, strike_price_range=None):
    query = f"""
        SELECT *
        FROM option_chain
        WHERE expiry_date = %s
    """
    params = [expiry_date]
    if strike_price_range:
        query += " AND strike_price BETWEEN %s AND %s"
        params.extend(strike_price_range)
    query += " ORDER BY strike_price ASC;"
    
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            st.error(f"Error fetching data from PostgreSQL: {e}")
            return None
        finally:
            conn.close()
    return None

# Profit/Loss Calculation for Straddle
def straddle_profit_loss(df, strike_price):
    ce = df[(df['strike_price'] == strike_price) & (df['option_type'] == 'CE')]
    pe = df[(df['strike_price'] == strike_price) & (df['option_type'] == 'PE')]
    if ce.empty or pe.empty:
        st.warning("Straddle cannot be formed with the given data.")
        return None
    
    combined_cost = ce['last_price'].values[0] + pe['last_price'].values[0]
    underlying_prices = np.linspace(strike_price * 0.8, strike_price * 1.2, 100)
    profit_loss = [
        max(price - strike_price, 0) + max(strike_price - price, 0) - combined_cost
        for price in underlying_prices
    ]
    
    return pd.DataFrame({'Underlying Price': underlying_prices, 'Profit/Loss': profit_loss})

# Profit/Loss Calculation for Iron Condor
def iron_condor_profit_loss(df, lower_strike, upper_strike):
    lower_ce = df[(df['strike_price'] == lower_strike) & (df['option_type'] == 'CE')]
    lower_pe = df[(df['strike_price'] == lower_strike) & (df['option_type'] == 'PE')]
    upper_ce = df[(df['strike_price'] == upper_strike) & (df['option_type'] == 'CE')]
    upper_pe = df[(df['strike_price'] == upper_strike) & (df['option_type'] == 'PE')]
    
    if lower_ce.empty or lower_pe.empty or upper_ce.empty or upper_pe.empty:
        st.warning("Iron Condor cannot be formed with the given data.")
        return None
    
    total_premium = (lower_ce['last_price'].values[0] + lower_pe['last_price'].values[0] -
                     upper_ce['last_price'].values[0] - upper_pe['last_price'].values[0])
    underlying_prices = np.linspace(lower_strike * 0.8, upper_strike * 1.2, 100)
    profit_loss = []
    
    for price in underlying_prices:
        pl = total_premium
        if price > upper_strike:
            pl -= (price - upper_strike)
        elif price < lower_strike:
            pl -= (lower_strike - price)
        profit_loss.append(pl)
    
    return pd.DataFrame({'Underlying Price': underlying_prices, 'Profit/Loss': profit_loss})

# Profit/Loss Calculation for Calendar Spread
def calendar_spread_profit_loss(df, strike_price):
    ce = df[(df['strike_price'] == strike_price) & (df['option_type'] == 'CE')]
    pe = df[(df['strike_price'] == strike_price) & (df['option_type'] == 'PE')]
    if ce.empty or pe.empty:
        st.warning("Calendar Spread cannot be formed with the given data.")
        return None
    
    premium_difference = ce['last_price'].values[0] - pe['last_price'].values[0]
    underlying_prices = np.linspace(strike_price * 0.8, strike_price * 1.2, 100)
    profit_loss = [premium_difference for _ in underlying_prices]
    
    return pd.DataFrame({'Underlying Price': underlying_prices, 'Profit/Loss': profit_loss})

# Plot Profit/Loss Data
def plot_profit_loss(df, strategy_name):
    st.subheader(f"{strategy_name} Strategy Profit/Loss")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['Underlying Price'], df['Profit/Loss'], label="Profit/Loss")
    ax.axhline(0, color='red', linestyle='--', linewidth=1)
    ax.set_xlabel("Underlying Price")
    ax.set_ylabel("Profit/Loss")
    ax.set_title(f"{strategy_name} Profit/Loss")
    ax.legend()
    ax.grid(True)
    st.pyplot(fig)

# Streamlit App
def main():
    st.title("📊 Options Trading Strategy Analyzer with Profit/Loss")
    
    # User Inputs
    expiry_date = st.sidebar.date_input("Select Expiry Date:", min_value=datetime(2020, 1, 1))
    strategy = st.sidebar.selectbox("Select Strategy:", ["Straddle", "Iron Condor", "Calendar Spread"])
    
    if strategy == "Straddle":
        strike_price = st.sidebar.number_input("Strike Price:", min_value=0.0, step=0.5)
    elif strategy == "Iron Condor":
        lower_strike = st.sidebar.number_input("Lower Strike Price:", min_value=0.0, step=0.5)
        upper_strike = st.sidebar.number_input("Upper Strike Price:", min_value=0.0, step=0.5)
    elif strategy == "Calendar Spread":
        strike_price = st.sidebar.number_input("Strike Price:", min_value=0.0, step=0.5)
    
    if st.sidebar.button("Analyze Strategy"):
        with st.spinner("Fetching data..."):
            df = fetch_option_data(expiry_date)
            if df is not None and not df.empty:
                st.success(f"Data fetched successfully! Total records: {len(df)}")
                
                if strategy == "Straddle":
                    profit_loss_df = straddle_profit_loss(df, strike_price)
                elif strategy == "Iron Condor":
                    profit_loss_df = iron_condor_profit_loss(df, lower_strike, upper_strike)
                elif strategy == "Calendar Spread":
                    profit_loss_df = calendar_spread_profit_loss(df, strike_price)
                
                if profit_loss_df is not None:
                    st.dataframe(profit_loss_df)
                    plot_profit_loss(profit_loss_df, strategy)
            else:
                st.warning("No data found for the selected criteria.")

if __name__ == "__main__":
    main()

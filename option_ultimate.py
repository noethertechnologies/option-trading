import time
import requests
import psycopg2
from scipy.stats import norm
from datetime import datetime
import numpy as np

# Constants
API_BASE_URL = "http://localhost:5000"
OPTION_CHAIN_ENDPOINT = f"{API_BASE_URL}/index-option-chain"
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

# Risk-free interest rate
r = 0.01

# Initialize PostgreSQL database
def init_db():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS option_chain (
        id SERIAL PRIMARY KEY,
        strike_price INTEGER,
        expiry_date TEXT,
        option_type TEXT,
        total_traded_volume INTEGER,
        open_interest REAL,
        change_in_open_interest REAL,
        implied_volatility REAL,
        last_price REAL,
        delta REAL,
        gamma REAL,
        theta REAL,
        bs_fair_value REAL,
        mcmc_fair_value REAL,
        timestamp TEXT
    );
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")

# Calculate Greeks and fair values
def calculate_greeks_and_fair_values(S0, K, T, r, IV, option_type):
    if T <= 0 or IV <= 0:
        return None, None, None, None
    
    d1 = (np.log(S0 / K) + (r + 0.5 * IV ** 2) * T) / (IV * np.sqrt(T))
    d2 = d1 - IV * np.sqrt(T)
    
    delta = norm.cdf(d1) if option_type == "call" else norm.cdf(d1) - 1
    gamma = norm.pdf(d1) / (S0 * IV * np.sqrt(T))
    theta = (- (S0 * norm.pdf(d1) * IV) / (2 * np.sqrt(T)) - 
             r * K * np.exp(-r * T) * (norm.cdf(d2) if option_type == "call" else norm.cdf(-d2))) / 365
    bs_fair_value = (S0 * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2) 
                     if option_type == "call" else K * np.exp(-r * T) * norm.cdf(-d2) - S0 * norm.cdf(-d1))
    
    return delta, gamma, theta, bs_fair_value

def calculate_mcmc_fair_value(S0, K, T, IV, option_type, num_simulations=10000):
    if T <= 0 or IV <= 0:
        return None

    try:
        # Ensure time to expiry is valid for simulation steps
        num_steps = max(int(T * 252), 1)
        daily_volatility = IV / np.sqrt(252)
        price_paths = S0 * np.exp(
            np.cumsum(
                np.random.normal(-0.5 * daily_volatility ** 2, daily_volatility, (num_simulations, num_steps)),
                axis=1
            )
        )
        final_prices = price_paths[:, -1]  # Final prices at expiry
        
        if option_type == "call":
            payoffs = np.maximum(final_prices - K, 0)
        else:
            payoffs = np.maximum(K - final_prices, 0)
        
        return np.mean(payoffs) * np.exp(-r * T)
    except Exception as e:
        print(f"Error in MCMC calculation: {e}")
        return None

# Fetch option chain data
def fetch_option_chain(symbol):
    try:
        response = requests.get(OPTION_CHAIN_ENDPOINT, params={"symbol": symbol})
        response.raise_for_status()
        return response.json()["optionChainData"]["records"]["data"]
    except Exception as e:
        print(f"Error fetching option chain data: {e}")
        return None

# Store option chain data in the database
def store_option_data(records):
    insert_query = """
    INSERT INTO option_chain (
        strike_price, expiry_date, option_type, total_traded_volume, open_interest,
        change_in_open_interest, implied_volatility, last_price, delta, gamma,
        theta, bs_fair_value, mcmc_fair_value, timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for record in records:
            cursor.execute(insert_query, record)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data stored successfully.")
    except Exception as e:
        print(f"Error storing data: {e}")

# Process option chain data
def process_option_chain(data, timestamp):
    processed_records = []
    for record in data:
        underlying_value = None
        
        for option_type in ["CE", "PE"]:
            if option_type in record:
                underlying_value = record[option_type].get("underlyingValue", underlying_value)

        if not underlying_value:
            print(f"Record missing 'underlyingValue': {record}")
            continue

        for option_type in ["CE", "PE"]:
            if option_type in record:
                option_data = record[option_type]
                total_traded_volume = option_data.get("totalTradedVolume", 0)
                open_interest = option_data.get("openInterest", 0)
                change_in_open_interest = option_data.get("changeinOpenInterest", 0)

                if total_traded_volume > 10000:
                    try:
                        S0 = underlying_value
                        K = option_data["strikePrice"]
                        IV = option_data["impliedVolatility"] / 100
                        T = max((datetime.strptime(option_data["expiryDate"], "%d-%b-%Y") - datetime.now()).days / 365, 0)
                        last_price = option_data["lastPrice"]
                        
                        delta, gamma, theta, bs_fair_value = calculate_greeks_and_fair_values(S0, K, T, r, IV, option_type.lower())
                        mcmc_fair_value = calculate_mcmc_fair_value(S0, K, T, IV, option_type.lower())
                        
                        processed_records.append((
                            K, option_data["expiryDate"], option_type, total_traded_volume, open_interest,
                            change_in_open_interest, IV, last_price, delta, gamma, theta,
                            bs_fair_value, mcmc_fair_value, timestamp
                        ))
                    except Exception as e:
                        print(f"Error processing record: {e}")
    return processed_records

# Main function
def main():
    init_db()
    symbol = "NIFTY"

    while True:
        print(f"Fetching option chain data for {symbol} at {datetime.now()}")
        data = fetch_option_chain(symbol)

        if data:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            processed_data = process_option_chain(data, timestamp)
            if processed_data:
                store_option_data(processed_data)
        else:
            print("No data fetched.")

        time.sleep(60)

if __name__ == "__main__":
    main()

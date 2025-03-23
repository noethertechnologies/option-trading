import numpy as np
import pandas as pd
from scipy.stats import norm

# Load and clean CSV data
df = pd.read_csv('option-chain-ED-NIFTY-09-Jan-2025.csv', header=0, skiprows=1)
df['STRIKE'] = pd.to_numeric(df['STRIKE'].str.replace(',', ''), errors='coerce')
df['VOLUME'] = pd.to_numeric(df['VOLUME'].str.replace(',', ''), errors='coerce')
df['OI'] = pd.to_numeric(df['OI'].str.replace(',', ''), errors='coerce')
df['IV'] = pd.to_numeric(df['IV'].str.replace(',', ''), errors='coerce')
df['LTP'] = pd.to_numeric(df['LTP'].str.replace(',', ''), errors='coerce')

# Define market inputs
r = 0.01  # Risk-free interest rate
T = (pd.to_datetime('09-Jan-2025') - pd.to_datetime('today')).days / 365  # Time to expiration

# Define Markov Chain states based on sentiment
states = ["bullish", "bearish", "neutral"]

# Function to calculate transition matrix based on market sentiment
def calculate_transition_matrix(volume, open_interest, implied_volatility):
    if volume > 1e5 and open_interest > 5e4 and implied_volatility > 0.2:
        transition_matrix = np.array([[0.6, 0.2, 0.2],
                                      [0.3, 0.5, 0.2],
                                      [0.3, 0.4, 0.3]])
    else:
        transition_matrix = np.array([[0.4, 0.3, 0.3],
                                      [0.3, 0.4, 0.3],
                                      [0.3, 0.3, 0.4]])
    return transition_matrix

# Simulate price paths using Markov chain
def simulate_price_paths(S0, T, transition_matrix, num_simulations, implied_volatility):
    price_paths = []
    for _ in range(num_simulations):
        price = S0
        path = [price]
        current_state = np.random.choice(states)
        
        for _ in range(int(T * 365)):  # Simulate daily price changes
            transition_probs = transition_matrix[states.index(current_state)]
            next_state = np.random.choice(states, p=transition_probs)
            
            if next_state == "bullish":
                price *= 1 + implied_volatility * 0.02
            elif next_state == "bearish":
                price *= 1 - implied_volatility * 0.02
            
            path.append(price)
            current_state = next_state
        
        price_paths.append(path)
    
    return np.array(price_paths)

# Greeks calculations
def calculate_delta(S0, strike_price, T, r, implied_volatility, option_type='call'):
    d1 = (np.log(S0 / strike_price) + (r + 0.5 * implied_volatility ** 2) * T) / (implied_volatility * np.sqrt(T))
    delta = norm.cdf(d1) if option_type == 'call' else norm.cdf(d1) - 1
    return delta

def calculate_gamma(S0, strike_price, T, r, implied_volatility):
    d1 = (np.log(S0 / strike_price) + (r + 0.5 * implied_volatility ** 2) * T) / (implied_volatility * np.sqrt(T))
    gamma = norm.pdf(d1) / (S0 * implied_volatility * np.sqrt(T))
    return gamma

def calculate_theta(S0, strike_price, T, r, implied_volatility, option_type='call'):
    d1 = (np.log(S0 / strike_price) + (r + 0.5 * implied_volatility ** 2) * T) / (implied_volatility * np.sqrt(T))
    d2 = d1 - implied_volatility * np.sqrt(T)
    theta = (- (S0 * norm.pdf(d1) * implied_volatility) / (2 * np.sqrt(T)) - 
             r * strike_price * np.exp(-r * T) * norm.cdf(d2 if option_type == 'call' else -d2))
    return theta / 365  # Convert to per-day

# Fair value calculation using Black-Scholes
def calculate_fair_value(S0, strike_price, T, r, implied_volatility, option_type):
    d1 = (np.log(S0 / strike_price) + (r + 0.5 * implied_volatility ** 2) * T) / (implied_volatility * np.sqrt(T))
    d2 = d1 - implied_volatility * np.sqrt(T)
    
    if option_type == 'call':
        fair_value = S0 * norm.cdf(d1) - strike_price * np.exp(-r * T) * norm.cdf(d2)
    else:
        fair_value = strike_price * np.exp(-r * T) * norm.cdf(-d2) - S0 * norm.cdf(-d1)
    
    return fair_value

# MCMC-based fair value calculation
def calculate_mcmc_fair_value(S0, strike_price, T, r, implied_volatility, option_type, num_simulations=10000):
    transition_matrix = calculate_transition_matrix(volume, open_interest, implied_volatility)
    price_paths = simulate_price_paths(S0, T, transition_matrix, num_simulations, implied_volatility)
    final_prices = price_paths[:, -1]  # Take the final price for each simulation
    
    if option_type == 'call':
        payoffs = np.maximum(final_prices - strike_price, 0)
    else:
        payoffs = np.maximum(strike_price - final_prices, 0)
    
    return np.mean(payoffs) * np.exp(-r * T)

# Process each strike to calculate Greeks and both fair values
results = []
for i in range(len(df)):
    try:
        if pd.notna(df.loc[i, 'STRIKE']) and pd.notna(df.loc[i, 'IV']):
            S0 = df.loc[i, 'STRIKE']
            strike_price = df.loc[i, 'STRIKE']
            volume = df.loc[i, 'VOLUME']
            open_interest = df.loc[i, 'OI']
            implied_volatility = df.loc[i, 'IV'] / 100  # Convert to decimal
            LTP = df.loc[i, 'LTP']
            option_type = 'call'  # Replace with actual option type column if available

            # Calculate Greeks
            delta = calculate_delta(S0, strike_price, T, r, implied_volatility, option_type)
            gamma = calculate_gamma(S0, strike_price, T, r, implied_volatility)
            theta = calculate_theta(S0, strike_price, T, r, implied_volatility, option_type)
            
            # Calculate fair values
            bs_fair_value = calculate_fair_value(S0, strike_price, T, r, implied_volatility, option_type)
            mcmc_fair_value = calculate_mcmc_fair_value(S0, strike_price, T, r, implied_volatility, option_type)
            
            # Append results
            results.append({
                'STRIKE': strike_price,
                'Delta': delta,
                'Gamma': gamma,
                'Theta': theta,
                'LTP': LTP,
                'BS Fair Value': bs_fair_value,
                'MCMC Fair Value': mcmc_fair_value,
                'Volume': volume,
                'Open Interest': open_interest,
                'Implied Volatility': implied_volatility
            })
    except KeyError as e:
        print(f"KeyError: {e} - check column names in CSV.")
    except Exception as e:
        print(f"Error at index {i}: {e}")

# Save results to a DataFrame and sort
results_df = pd.DataFrame(results)
most_profitable_strikes = results_df.sort_values(by='MCMC Fair Value', ascending=False)

# Write results to a .txt file
with open('option_greeks_fair_values.txt', 'w') as f:
    f.write(most_profitable_strikes[['STRIKE', 'Delta', 'Gamma', 'Theta', 'LTP', 'BS Fair Value', 'MCMC Fair Value', 'Volume', 'Open Interest', 'Implied Volatility']].to_string(index=False))

# Display for verification
print(most_profitable_strikes[['STRIKE', 'Delta', 'Gamma', 'Theta', 'LTP', 'BS Fair Value', 'MCMC Fair Value', 'Volume', 'Open Interest', 'Implied Volatility']])

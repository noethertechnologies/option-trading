import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Constants
DB_CONFIG = {
    "dbname": "option_data",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

TABLE_NAME = "option_data"

# Function to establish a PostgreSQL connection
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
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
            st.success(f"Database table '{TABLE_NAME}' is ready.")
        except Exception as e:
            st.error(f"Error creating '{TABLE_NAME}' table: {e}")
        finally:
            conn.close()

# Function to insert data into the database
def insert_csv_data(file_path):
    conn = get_db_connection()
    if conn:
        try:
            data = pd.read_csv(file_path)
            data = data.rename(columns=str.lower)  # Ensure columns are lowercase to match DB fields
            insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                strike_price, expiry_date, option_type, open_interest, change_in_open_interest,
                pchange_in_open_interest, total_traded_volume, implied_volatility, last_price,
                change, p_change, total_buy_quantity, total_sell_quantity, bid_qty, bid_price,
                ask_qty, ask_price, underlying_value, timestamp
            ) VALUES %s
            ON CONFLICT (strike_price, option_type, expiry_date, timestamp) DO NOTHING;
            """
            # Prepare data for bulk insertion
            records = data.to_records(index=False)
            values = list(records)

            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
            conn.commit()
            st.success("Data successfully inserted into the database.")
        except Exception as e:
            st.error(f"Error inserting data: {e}")
        finally:
            conn.close()

# Streamlit Dashboard
def main():
    st.title("📊 Option Chain Data Dashboard")
    st.sidebar.title("Options")

    # Ensure the table exists
    st.sidebar.button("Create Table", on_click=create_option_chain_table)

    # File upload for CSV
    uploaded_file = st.sidebar.file_uploader("Upload CSV File", type=["csv"])
    if uploaded_file:
        st.sidebar.button("Insert Data", on_click=lambda: insert_csv_data(uploaded_file))

    # Display data
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 100;")
                rows = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=colnames)

                st.subheader("Option Chain Data (Sample)")
                st.write(df)
                
        except Exception as e:
            st.error(f"Error fetching data: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()

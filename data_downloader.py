import streamlit as st
import pandas as pd
import psycopg2

# Database Configuration
DB_CONFIG = {
    "dbname": "optiondb",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": 5432,
}

# Function to establish a database connection
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

# Function to fetch data from the database
def fetch_data():
    query = """
        SELECT *
        FROM option_chain
        WHERE total_traded_volume > 10000
        ORDER BY timestamp DESC, strike_price ASC;
    """
    conn = get_db_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
        finally:
            conn.close()
    else:
        return None

# Streamlit app
def main():
    st.title("📊 Option Chain Data Viewer and Exporter")
    
    # Fetch data on button click
    if st.button("Fetch Data"):
        with st.spinner("Fetching data from the database..."):
            df = fetch_data()
            if df is not None and not df.empty:
                st.success(f"Data fetched successfully! Total records: {len(df)}")
                
                # Display download button for the DataFrame
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download data as CSV",
                    data=csv,
                    file_name='option_chain_data.csv',
                    mime='text/csv',
                )
            else:
                st.warning("No data found with total traded volume greater than 10000.")

if __name__ == "__main__":
    main()

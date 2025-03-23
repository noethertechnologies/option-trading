import psycopg2
from psycopg2 import sql
import streamlit as st
import pandas as pd

# Database connection parameters
db_params = {
    "dbname": "stock_dashboard",
    "user": "root",
    "password": "arka1256",
    "host": "localhost",
    "port": "5432",
}

def fetch_table_data(conn, table_name):
    """Fetches data from a specified table."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except psycopg2.Error as e:
        st.error(f"Error fetching data from table '{table_name}': {e}")
        return None

def get_all_tables(conn):
    """Gets a list of all tables in the database."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            return [row[0] for row in cursor.fetchall()]
    except psycopg2.Error as e:
        st.error(f"Error retrieving table list: {e}")
        return []

def main():
    st.title("📊 PostgreSQL Database Viewer")

    # Connect to the database
    try:
        with psycopg2.connect(**db_params) as conn:
            tables = get_all_tables(conn)

            if not tables:
                st.warning("No tables found in the database.")
                return

            # Table selection
            selected_table = st.selectbox("Select a table to view:", tables)
            if selected_table:
                # Fetch and display table data
                df = fetch_table_data(conn, selected_table)
                if df is not None:
                    st.dataframe(df)

                    # Downloadable CSV
                    csv = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="Download Table as CSV",
                        data=csv,
                        file_name=f"{selected_table}.csv",
                        mime="text/csv",
                    )

    except psycopg2.OperationalError as e:
        st.error(f"Database connection error: {e}")
    except Exception as e:
        st.exception(e)

if __name__ == "__main__":
    main()

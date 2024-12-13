import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect(
        dbname="etl_policeincidents_db",
        user="postgres",
        password="etlpoliceincidents24$$",
        host="etl-policeincidents.c1oyg6c88jj2.us-east-2.rds.amazonaws.com",
        port="5432"
    )
    print("Connection successful!")
    query = "SELECT * FROM police_incidents LIMIT 10;"
    df = pd.read_sql_query(query, conn)
    print(df)
    conn.close()
except Exception as e:
    print(f"Error: {e}")

import streamlit as st
import psycopg2
import pandas as pd

# Título del Dashboard
st.title("Police Incidents Dashboard")

# Conexión a la base de datos PostgreSQL
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname="etl_policeincidents_db",
        user="postgres",
        password="etlpoliceincidents24$$",
        host="etl-policeincidents.c1oyg6c88jj2.us-east-2.rds.amazonaws.com",
        port="5432"
    )

# Consultar datos
def fetch_data(query):
    conn = get_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Consulta y visualización de los datos
try:
    query = "SELECT * FROM police_incidents LIMIT 100;"
    data = fetch_data(query)

    st.write("### Data Preview")
    st.dataframe(data)

    # Visualización interactiva (ejemplo)
    st.write("### Incidents by Type")
    incident_counts = data["incident_type"].value_counts()
    st.bar_chart(incident_counts)
except Exception as e:
    st.error(f"An error occurred: {e}")


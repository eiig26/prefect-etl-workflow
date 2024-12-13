import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# Conection with PostgreSQL
conn = psycopg2.connect(
    dbname="etl_policeincidents_db",
    user="postgres",
    password="etlpoliceincidents24$$",
    host="etl-policeincidents.c1oyg6c88jj2.us-east-2.rds.amazonaws.com",
    port="5432"
)

# Load Data
def load_data():
    query = "SELECT * FROM police_incidents"
    df = pd.read_sql(query, conn)
    return df

df = load_data()

# Date filter
st.sidebar.header('Filter')
min_date = df['datetime_logged'].min()
max_date = df['datetime_logged'].max()
selected_date = st.sidebar.date_input("Select Date", value=max_date, min_value=min_date, max_value=max_date)

df_filtered = df[df['datetime_logged'].dt.date <= pd.to_datetime(selected_date).date()]

st.title("Police Incidents Dashboard in Worcester, Massachusetts")

# Total of Incidents
total_incidents = df_filtered.shape[0]
st.sidebar.metric(label="Total of Incidents", value=total_incidents)

# Top 5 Locations
top_locations = df_filtered['location'].value_counts().head(5)

# Top 5 Left Panel
st.sidebar.subheader('Top 5 Locations with more Police incidents')
st.sidebar.write(top_locations)

# Graph Incident "incident_category"
incident_category_counts = df_filtered['incident_category'].value_counts()
fig_category = px.bar(incident_category_counts, 
                      x=incident_category_counts.index, 
                      y=incident_category_counts.values, 
                      labels={'x': 'Incident Category', 'y': 'Total of Incidents'},
                      title='Distribution of Incidents by Category')
st.plotly_chart(fig_category)

# Graph Priority "response_priority"
response_priority_counts = df_filtered['response_priority'].value_counts()
response_priority_counts = response_priority_counts[response_priority_counts.index != "Unknown"]
priority_colors = {
    "High": "red",
    "Medium": "yellow",
    "Low": "green"
}
fig_priority = px.bar(response_priority_counts,
                    x=response_priority_counts.index,
                    y=response_priority_counts.values,
                    labels={'x': 'Response Priority', 'y': 'Total of Incidents'},
                    title='Distribution of Incidents by Response Priority',
                    color=response_priority_counts.index, 
                    color_discrete_map=priority_colors)
st.plotly_chart(fig_priority)


# Graph "time_of_day"
time_day_counts = df_filtered['time_of_day'].value_counts().reset_index()
time_day_counts.columns = ['time_of_day', 'count']  
fig_timeday_pie = px.pie(time_day_counts,
                          names='time_of_day',  
                          values='count',      
                          labels={'time_of_day': 'Hora del Día', 'count': 'Total de Incidentes'},
                          title='Distribution of Incidents by Time of the day',
                          color_discrete_sequence=px.colors.qualitative.Set3) 
st.plotly_chart(fig_timeday_pie)

st.write("### Data Preview")
st.dataframe(df_filtered)

# 5. About block
st.sidebar.markdown("### About:")
st.sidebar.markdown("""Data source: [City of Worcester, MA - Police Incident Data - 2024](https://opendata.worcesterma.gov/datasets/worcesterma::police-incident-data-2024/explore)""")  
st.sidebar.markdown("""Author: Esiquio Iglesias Guerra""")
st.sidebar.markdown("""Msc. in Data Analytics and Computational Social Science""")
st.sidebar.markdown("""University of Massachusetts Amherst""")


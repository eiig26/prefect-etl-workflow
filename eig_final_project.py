import requests
import json
import pandas as pd
from prefect import task, flow
import psycopg2
from psycopg2.extras import execute_values


# Task 1: Extract data from API
@task
def extract_data(api_url: str, output_file: str) -> None:
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for HTTP issues
        with open(output_file, "w") as file:
            json.dump(response.json(), file)
        print(f"Data extracted and saved to {output_file}")
    except Exception as e:
        print(f"Error during data extraction: {e}")
        raise

# Task 2: Transform the data
@task
def transform_data(input_file: str, output_file: str) -> None:
    try:
        # Load JSON data
        with open(input_file, "r") as file:
            data = json.load(file)

        # Extract features
        features = data.get("features", [])
        if not features:
            raise ValueError("No features found in the JSON file.")

        # Normalize and flatten the 'properties' field
        incidents = pd.json_normalize([feature.get("properties", {}) for feature in features])

        # Select columns of interest
        columns_to_keep = [
            "Incident_Number",
            "Date_Logged",
            "Time_Logged",
            "Department",
            "Incident_Type",
            "Location",
            "ZipCode",
            "Action_Taken",
            "Officer"
        ]
        incidents = incidents[columns_to_keep]

        # Combine Date and Time into a Datetime column
        incidents["Datetime_Logged"] = pd.to_datetime(
            incidents["Date_Logged"] + " " + incidents["Time_Logged"],
            errors="coerce"
        )
        incidents.drop(columns=["Date_Logged", "Time_Logged"], inplace=True)

        # Classify the time of the day
        def classify_time_of_day(dt):
            if pd.isnull(dt):
                return "Unknown"
            hour = dt.hour
            if 5 <= hour < 12:
                return "Morning"
            elif 12 <= hour < 17:
                return "Afternoon"
            elif 17 <= hour < 21:
                return "Evening"
            else:
                return "Night"

        incidents["Time_Of_Day"] = incidents["Datetime_Logged"].apply(classify_time_of_day)
        incidents["Datetime_Logged"] = incidents["Datetime_Logged"].fillna(pd.Timestamp.min)

        # Categorize incidents based on the incident type
        def categorize_incident(incident):
            categories = {
                'AMB': 'Medical',
                'DIP': 'Public Disorder',
                'CKW': 'Welfare Check',
                'ACC': 'Traffic Incidents',
                'ACI': 'Traffic Incidents',
                'DIS': 'Public Disorder',
                'ALC': 'Alarm',
                'ALR': 'Alarm',
                'ASN': 'Administrative',
                'ASC': 'Citizen Assistance',
                'ASD': 'Citizen Assistance',
                'MVC': 'Traffic Violations',
                'LAU': 'Theft',
                'WAR': 'Legal',
                'ASL': 'Violent Incidents',
                'CEP': 'Community Engagement',
                'CPP': 'Community Engagement',
                'CAN': 'Cancelled Calls',
                'THR': 'Threats',
                'TOW': 'Tow Requests',
            }
            return categories.get(incident[:3], 'Other')  # Fallback to 'Other'

        incidents['incident_category'] = incidents['Incident_Type'].apply(categorize_incident)

        # Assign priority to incidents
        def assign_priority(incident):
            high_priority = ['AMB', 'ACI', 'ASL', 'THR']
            medium_priority = ['ACC', 'DIS', 'DIP', 'MVC']
            low_priority = ['CEP', 'CPP', 'CAN', 'ALC', 'ALR']
            if incident[:3] in high_priority:
                return 'High'
            elif incident[:3] in medium_priority:
                return 'Medium'
            elif incident[:3] in low_priority:
                return 'Low'
            else:
                return 'Unknown'

        incidents['response_priority'] = incidents['Incident_Type'].apply(assign_priority)

        
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise
# Save transformed data 
    incidents.to_csv(output_file, index=False)
    print(f"Transformed data saved to {output_file}")
    return incidents


# Task 3: Load the data (Optional for database or dashboard integration)

@task
def load_data_to_postgres(df: pd.DataFrame, db_params: dict) -> None:
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Ensure table exists and has the correct structure
        table_name = "police_incidents"
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            incident_number TEXT PRIMARY KEY,
            department TEXT,
            incident_type TEXT,
            location TEXT,
            zipcode TEXT,
            action_taken TEXT,
            officer TEXT,
            datetime_logged TIMESTAMP,
            time_of_day TEXT,
            incident_category TEXT,
            response_priority TEXT
        );
        """
        cursor.execute(create_table_query)

        # Insert data
        columns = [
            "Incident_Number", "Department", "Incident_Type", "Location",
            "ZipCode", "Action_Taken", "Officer", "Datetime_Logged",
            "Time_Of_Day", "incident_category", "response_priority"
        ]
        values = df[columns].values.tolist()
        execute_values(cursor, f"""
            INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s
            ON CONFLICT (Incident_Number) DO NOTHING;
        """, values)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully loaded to PostgreSQL.")
    except Exception as e:
        print(f"Error loading data: {e}")
        raise


# Main ETL Flow
@flow
def etl_pipeline(api_url: str, raw_file: str, transformed_file: str, db_params: dict):
    extract_data(api_url, raw_file)
    df = transform_data(raw_file, transformed_file)
    load_data_to_postgres(df, db_params)

# Run the pipeline
if __name__ == "__main__":
    API_URL = "https://services1.arcgis.com/j8dqo2DJE7mVUBU1/arcgis/rest/services/Police_Incident_Data_2024/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    RAW_FILE = "police_incidents.json"
    TRANSFORMED_FILE = "police_incidents_transformed.csv"

    # Database connection parameters
    DB_PARAMS = {
    "dbname": "etl_policeincidents_db",
    "user": "postgres",
    "password": "etlpoliceincidents24$$",
    "host": "etl-policeincidents.c1oyg6c88jj2.us-east-2.rds.amazonaws.com",
    "port": "5432"
}
    etl_pipeline(API_URL, RAW_FILE, TRANSFORMED_FILE, DB_PARAMS)

               
                      




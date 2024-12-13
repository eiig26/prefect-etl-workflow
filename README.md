# Police Incident Data ETL Pipeline and Dashboard

## Overview
This project involves creating an ETL pipeline and an interactive dashboard for analyzing police incident data in Worcester, Massachusetts. 

## Data Source
The dataset used for this project is **Police Incident Data - 2024**, provided by the Department of Innovation & Technology, City of Worcester, MA.  
**Link:** [Police Incident Data - 2024](https://opendata.worcesterma.gov/datasets/worcesterma::police-incident-data-2024/about)

---

## ETL Pipeline

### Steps
1. **Extract**: Data is fetched from an API in GeoJSON format and saved locally as a JSON file.  
2. **Transform**: 
   - Normalize and flatten the JSON data into a structured tabular format.  
   - Select relevant columns like `Incident_Number`, `Incident_Type`, `Location`, etc.  
   - Combine date and time columns into a `Datetime_Logged` column.  
   - Categorize incidents based on types and assign priority levels.  
   - Add new features, such as `Time_Of_Day` and `incident_category`.
3. **Load**: The transformed data is uploaded to a PostgreSQL database table named `police_incidents`.

### Automation
The pipeline is implemented using a Python script with the `Prefect` library, enabling task orchestration and automation. Each step of the pipeline is defined as a task within a `flow`.

## Dashboard
The dashboard is built using Streamlit to provide an interactive view of the processed data. Users can filter and visualize incidents based on date, location, and various classifications.
**Link:** [Streamlit Dashboard](https://eiig26-prefect-etl-workflow-app-3o6t54.streamlit.app/)

Dashboard Features
- Date Filter: Filter incidents based on a specific date.
- Total Incidents: Display the total number of incidents for the selected date range.
- Top 5 Locations: Show the locations with the highest number of incidents.
- Category Distribution: Bar chart showing incidents by category.
- Priority Distribution: Bar chart showing incidents by response priority, categorized into High, Medium, and Low.
- Time of Day Analysis: Pie chart depicting the distribution of incidents across different times of the day.
- Data Preview: Display a table of filtered data.

### Tools and Technologies
- **Python Libraries**: `pandas`, `json`, `psycopg2`, `Prefect`, `Streamlit`
- **Database**: PostgreSQL  
- **API**: RESTful API provided by the City of Worcester  

### Command to Run the Pipeline
```bash
python etl_pipeline.py

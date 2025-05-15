import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime
from streamlit.components.v1 import html
from dotenv import load_dotenv
import os

load_dotenv()

# AWS Configuration
ATHENA_DATABASE = os.getenv('athena_database')
S3_OUTPUT_LOCATION = os.getenv('s3_Output')
REGION = os.getenv('region')
AWS_ACCESS_KEY_ID = os.getenv('access_key_id')
AWS_SECRET_ACCESS_KEY = os.getenv('secret_access_key')

# Initialize Boto3 Athena client
athena_client = boto3.client('athena',
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                              region_name=REGION)

# Function to execute Athena query and return results as DataFrame
def run_athena_query(query):
    query_execution = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
    )
    query_execution_id = query_execution['QueryExecutionId']
    
    # Poll for query completion
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        # Convert results to DataFrame
        columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:  # Skip header row
            rows.append([data.get('VarCharValue', '') for data in row['Data']])
        return pd.DataFrame(rows, columns=columns)
    else:
        raise Exception(f"Query failed with status: {status}")

# Streamlit app
st.set_page_config(page_title="Telco Network Metrics Dashboard", layout="wide")
st.title("Real-Time Telco Network Metrics Dashboard")

# Auto-refresh configuration
refresh_interval = st.slider("Refresh Interval (minutes)", min_value=1, max_value=30, value=5)
refresh_seconds = refresh_interval * 60

# JavaScript for auto-refresh
refresh_script = f"""
<script>
    setTimeout(function() {{
        window.location.reload();
    }}, {refresh_seconds * 1000});
</script>
"""
html(refresh_script)

# Display last refresh time
st.write(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Query 1: Average Signal Strength per Operator
signal_query = """
SELECT operator, avg_signal_strength
FROM avg_precision
"""
try:
    signal_df = run_athena_query(signal_query)
    signal_df['avg_signal_strength'] = signal_df['avg_signal_strength'].astype(float)
    
    # Visualization: Bar chart for signal strength
    fig_signal = px.bar(
        signal_df,
        x='operator',
        y='avg_signal_strength',
        title='Average Signal Strength per Operator (dBm)',
        labels={'avg_signal_strength': 'Average Signal Strength (dBm)', 'operator': 'Operator'},
        color='avg_signal_strength',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_signal, use_container_width=True)
except Exception as e:
    st.error(f"Error fetching signal strength data: {str(e)}")

# Query 2: Average GPS Precision per Operator
gps_query = """
SELECT operator, avg_gps_precision
FROM avg_precision
"""
try:
    gps_df = run_athena_query(gps_query)
    gps_df['avg_gps_precision'] = gps_df['avg_gps_precision'].astype(float)
    
    # Visualization: Bar chart for GPS precision
    fig_gps = px.bar(
        gps_df,
        x='operator',
        y='avg_gps_precision',
        title='Average GPS Precision per Operator (meters)',
        labels={'avg_gps_precision': 'Average GPS Precision (meters)', 'operator': 'Operator'},
        color='avg_gps_precision',
        color_continuous_scale='Greens'
    )
    st.plotly_chart(fig_gps, use_container_width=True)
except Exception as e:
    st.error(f"Error fetching GPS precision data: {str(e)}")

# Query 3: Count of Network Statuses per Postal Code
# Try querying with network_status first
status_query = """
SELECT postal_code, network_status, status_count
FROM status_count
"""
try:
    status_df = run_athena_query(status_query)
    status_df['status_count'] = status_df['status_count'].astype(int)
    
    # Visualization: Heatmap for network statuses
    status_pivot = status_df.pivot(index='postal_code', columns='network_status', values='status_count').fillna(0)
    fig_status = go.Figure(data=go.Heatmap(
        z=status_pivot.values,
        x=status_pivot.columns,
        y=status_pivot.index,
        colorscale='Viridis',
        showscale=True
    ))
    fig_status.update_layout(
        title='Network Status Counts by Postal Code',
        xaxis_title='Network Status',
        yaxis_title='Postal Code',
        height=600
    )
    st.plotly_chart(fig_status, use_container_width=True)
    
    # Display raw data table
    st.subheader("Network Status Counts Data")
    st.dataframe(status_df)
except Exception as e:
    # If query fails (e.g., no network_status column), try alternative query
    st.warning(f"Failed to fetch status data with network_status: {str(e)}. Attempting alternative query...")
    alt_status_query = """
    SELECT postal_code, status_count
    FROM status_count
    """
    try:
        status_df = run_athena_query(alt_status_query)
        status_df['status_count'] = status_df['status_count'].astype(int)
        
        # Visualization: Bar chart for status counts
        fig_status = px.bar(
            status_df,
            x='postal_code',
            y='status_count',
            title='Network Status Counts by Postal Code',
            labels={'status_count': 'Status Count', 'postal_code': 'Postal Code'},
            color='status_count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_status, use_container_width=True)
        
        # Display raw data table
        st.subheader("Network Status Counts Data")
        st.dataframe(status_df)
    except Exception as alt_e:
        st.error(f"Error fetching status count data: {str(alt_e)}")
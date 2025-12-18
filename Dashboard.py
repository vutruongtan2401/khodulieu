import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import warnings
from sqlalchemy import create_engine
from urllib.parse import quote_plus

warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(page_title="Air Quality Dashboard", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Database connection settings in sidebar
st.sidebar.title("🔧 Database Connection")
st.sidebar.divider()

db_host = st.sidebar.text_input("Host", value="aws-1-ap-southeast-1.pooler.supabase.com")
db_port = st.sidebar.text_input("Port", value="5432")
db_name = st.sidebar.text_input("Database", value="postgres")
db_user = st.sidebar.text_input("Username", value="postgres.bkqhsxdynslfdtkcucij")
db_password = st.sidebar.text_input("Password", type="password", value="Duy@12345")

st.sidebar.divider()
st.sidebar.title("🔍 Filters")
st.sidebar.divider()

# Load data from database
@st.cache_data
def load_data_from_db():
    try:
        # Create connection string with safe URL-encoding for credentials
        encoded_user = quote_plus(db_user)
        encoded_password = quote_plus(db_password)
        connection_string = (
            f"postgresql+psycopg2://{encoded_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True)
        
        # Load data from tables using SQL queries (more robust than read_sql_table)
        fact_air = pd.read_sql_query('SELECT * FROM "Fact_AirQuality"', engine)
        fact_forecast = pd.read_sql_query('SELECT * FROM "Fact_Forecast"', engine)
        dim_date = pd.read_sql_query('SELECT * FROM "Dim_Date"', engine)
        dim_location = pd.read_sql_query('SELECT * FROM "Dim_Location"', engine)
        dim_time = pd.read_sql_query('SELECT * FROM "Dim_Time"', engine)
        dim_parameter = pd.read_sql_query('SELECT * FROM "Dim_Parameter"', engine)
        
        return fact_air, fact_forecast, dim_date, dim_location, dim_time, dim_parameter, None
    except Exception as e:
        return None, None, None, None, None, None, str(e)

# Load all data
fact_air, fact_forecast, dim_date, dim_location, dim_time, dim_parameter, db_error = load_data_from_db()

if db_error:
    st.error(f"❌ Database connection error: {db_error}")
    st.info("💡 Please check your database credentials in the sidebar and refresh the page.")
    st.stop()

if fact_air is None:
    st.error("❌ Unable to load data from database")
    st.stop()

# Merge data to get meaningful information
def prepare_data():
    # Merge fact_air with dimensions
    df = fact_air.merge(dim_date, on='DateKey', how='left')
    df = df.merge(dim_time, on='TimeKey', how='left')
    df = df.merge(dim_parameter, on='ParameterKey', how='left')
    df = df.merge(dim_location, on='LocationKey', how='left')
    
    # Convert dates
    if 'FullDate' in df.columns:
        # DB may already return datetime/date types; avoid forcing a specific format
        df['FullDate'] = pd.to_datetime(df['FullDate'], errors='coerce')
    
    # Convert Value column to numeric
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    
    # Remove rows with NaN values in Value column
    df = df.dropna(subset=['Value'])
    
    return df

data = prepare_data()

# Date range filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(data['FullDate'].min().date(), data['FullDate'].max().date()),
    min_value=data['FullDate'].min().date(),
    max_value=data['FullDate'].max().date()
)

# Parameter filter
parameters = st.sidebar.multiselect(
    "Select Parameters",
    options=sorted(data['ParameterName'].unique()),
    default=sorted(data['ParameterName'].unique())
)

# Filter data based on selections
filtered_data = data[
    (data['FullDate'].dt.date >= date_range[0]) &
    (data['FullDate'].dt.date <= date_range[1]) &
    (data['ParameterName'].isin(parameters))
].copy()

# Main dashboard
st.title("🌍 Air Quality Monitoring Dashboard")
location_name = dim_location['LocationName'].iloc[0] if len(dim_location) > 0 else 'N/A'
st.markdown(f"**Location:** {location_name} | **Last Updated:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
#st.markdown(f"**Data Source:** PostgreSQL Database | **Connection:** {db_host}:{db_port}")
st.divider()

# KPI Metrics
col1, col2, col3, col4 = st.columns(4)

# Calculate metrics
if len(filtered_data) > 0:
    # Average by Day
    avg_day = filtered_data.groupby('FullDate')['Value'].mean().mean()
    
    # Average by Week
    filtered_data_copy = filtered_data.copy()
    filtered_data_copy['Week'] = filtered_data_copy['FullDate'].dt.isocalendar().week
    avg_week = filtered_data_copy.groupby('Week')['Value'].mean().mean()
    
    # Average by Month
    filtered_data_copy['Month'] = filtered_data_copy['FullDate'].dt.month
    avg_month = filtered_data_copy.groupby('Month')['Value'].mean().mean()
    
    median_value = filtered_data['Value'].median()
else:
    avg_day = 0
    avg_week = 0
    avg_month = 0
    median_value = 0

with col1:
    st.metric(
        label="📆 Average Day",
        value=f"{avg_day:,.0f}",
        delta="Daily Avg" if avg_day > 0 else "→ No Data"
    )

with col2:
    st.metric(
        label="📅 Average Week",
        value=f"{avg_week:,.0f}",
        delta="Weekly Avg" if avg_week > 0 else "→ No Data"
    )

with col3:
    st.metric(
        label="📋 Average Month",
        value=f"{avg_month:,.0f}",
        delta="Monthly Avg" if avg_month > 0 else "→ No Data"
    )

with col4:
    st.metric(
        label="📊 Median Value",
        value=f"{median_value:,.0f}",
        delta="Mid-Point" if median_value > 0 else "→ No Data"
    )

st.divider()

# Charts Section
col1, col2 = st.columns(2)

# Time series chart
with col1:
    st.subheader("📈 Trend Over Time")
    
    if len(filtered_data) > 0:
        # Group by date and parameter
        trend_data = filtered_data.groupby(['FullDate', 'ParameterName'])['Value'].mean().reset_index()
        
        fig_trend = px.line(
            trend_data,
            x='FullDate',
            y='Value',
            color='ParameterName',
            title="Air Quality Parameters - Daily Average",
            labels={'FullDate': 'Date', 'Value': 'Value (µg/m³)', 'ParameterName': 'Parameter'}
        )
        fig_trend.update_layout(
            hovermode='x unified',
            height=400,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No data available for selected filters")

# Distribution by parameter
with col2:
    st.subheader("📊 Distribution by Parameter")
    
    if len(filtered_data) > 0:
        param_data = filtered_data.groupby('ParameterName')['Value'].mean().sort_values(ascending=False)
        
        fig_bar = px.bar(
            x=param_data.index,
            y=param_data.values,
            labels={'x': 'Parameter', 'y': 'Average Value (µg/m³)'},
            title="Average Values by Parameter",
            color=param_data.values,
            color_continuous_scale="Viridis"
        )
        fig_bar.update_layout(
            hovermode='x',
            height=400,
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No data available for selected filters")

st.divider()

# Hourly pattern analysis
col1, col2 = st.columns(2)

with col1:
    st.subheader("⏰ Hourly Pattern")
    
    if len(filtered_data) > 0:
        hourly_data = filtered_data.groupby('Hour')['Value'].mean().reset_index()
        
        fig_hourly = go.Figure()
        fig_hourly.add_trace(go.Scatter(
            x=hourly_data['Hour'],
            y=hourly_data['Value'],
            mode='lines+markers',
            name='Average Value',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        ))
        fig_hourly.update_layout(
            title="Hourly Air Quality Pattern",
            xaxis_title="Hour of Day",
            yaxis_title="Value (µg/m³)",
            hovermode='x unified',
            height=400,
            margin=dict(l=0, r=0, t=30, b=0),
            xaxis=dict(tickmode='linear', tick0=0, dtick=4)
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    else:
        st.info("No data available for selected filters")

# Forecast vs Actual
with col2:
    st.subheader("🔮 Forecast vs Actual")
    
    # Prepare forecast data
    forecast_data = fact_forecast.merge(dim_date, on='DateKey', how='left')
    forecast_data = forecast_data.merge(dim_parameter, on='ParameterKey', how='left')
    forecast_data = forecast_data.merge(dim_time, on='TimeKey', how='left')
    
    if 'FullDate' in forecast_data.columns:
        forecast_data['FullDate'] = pd.to_datetime(forecast_data['FullDate'], format='%d/%m/%Y', errors='coerce')
    
    # Convert Value column to numeric
    forecast_data['Value'] = pd.to_numeric(forecast_data['Value'], errors='coerce')
    forecast_data = forecast_data.dropna(subset=['Value'])
    
    # Get latest forecast
    if len(forecast_data) > 0 and 'Hour' in forecast_data.columns and len(filtered_data) > 0:
        hourly_data = filtered_data.groupby('Hour')['Value'].mean().reset_index()
        latest_forecast = forecast_data.groupby('Hour')['Value'].mean().reset_index()
        latest_actual = hourly_data.copy()
        latest_actual.columns = ['Hour', 'Actual']
        
        comparison = latest_forecast.merge(
            latest_actual,
            on='Hour',
            how='left'
        )
        comparison.columns = ['Hour', 'Forecast', 'Actual']
        
        fig_forecast = go.Figure()
        fig_forecast.add_trace(go.Scatter(
            x=comparison['Hour'],
            y=comparison['Forecast'],
            mode='lines+markers',
            name='Forecast',
            line=dict(color='#ff7f0e', width=2, dash='dash')
        ))
        fig_forecast.add_trace(go.Scatter(
            x=comparison['Hour'],
            y=comparison['Actual'],
            mode='lines+markers',
            name='Actual',
            line=dict(color='#2ca02c', width=2)
        ))
        fig_forecast.update_layout(
            title="Forecast vs Actual Values",
            xaxis_title="Hour of Day",
            yaxis_title="Value (µg/m³)",
            hovermode='x unified',
            height=400,
            margin=dict(l=0, r=0, t=30, b=0),
            xaxis=dict(tickmode='linear', tick0=0, dtick=4)
        )
        st.plotly_chart(fig_forecast, use_container_width=True)
    else:
        st.info("No forecast data available")

st.divider()

# Data statistics
st.subheader("📋 Data Statistics")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Records", f"{len(filtered_data):,}")

with col2:
    st.metric("Date Range", f"{(date_range[1] - date_range[0]).days} days")

with col3:
    st.metric("Parameters Selected", len(parameters))

# Detailed data table
st.subheader("🔍 Detailed Data")

if len(filtered_data) > 0:
    display_data = filtered_data[['FullDate', 'TimeStr', 'ParameterName', 'Value', 'Unit', 'LocationName']].copy()
    display_data.columns = ['Date', 'Time', 'Parameter', 'Value', 'Unit', 'Location']
    display_data = display_data.sort_values('Date', ascending=False)
    
    st.dataframe(
        display_data.head(100),
        use_container_width=True,
        hide_index=True
    )
    
    st.caption(f"Showing top 100 records out of {len(display_data):,} total records")
else:
    st.info("No data available for the selected filters")

# Footer
st.divider()
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 0.8em; margin-top: 2rem;'>
        <p>Air Quality Monitoring Dashboard | Data Source: PostgreSQL Database</p>
        <p>Last Updated: """ + datetime.now().strftime('%d/%m/%Y %H:%M:%S') + """</p>
    </div>
    """,
    unsafe_allow_html=True
)

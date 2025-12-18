import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import os
import warnings
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Unified Analytics Dashboard", page_icon=":bar_chart:", layout="wide")

st.title(" :bar_chart: Unified Data Analytics Dashboard")
st.markdown('<style>div.block-container{padding-top:1rem;}</style>', unsafe_allow_html=True)

# Sidebar for data source selection
st.sidebar.header("📊 Data Source")
data_source = st.sidebar.radio("Choose data source:", ["PostgreSQL Database", "CSV Files"])

all_data = {}

if data_source == "PostgreSQL Database":
    st.sidebar.subheader("🔐 Database Connection")
    
    db_host = st.sidebar.text_input("Host", value="aws-1-ap-southeast-1.pooler.supabase.com")
    db_port = st.sidebar.text_input("Port", value="5432")
    db_name = st.sidebar.text_input("Database", value="postgres")
    db_user = st.sidebar.text_input("Username", value="postgres.ayfafsqyjucvbkuwqxxy")
    db_password = st.sidebar.text_input("Password", type="password", value="Duy@12345")
    
    if st.sidebar.button("🔌 Connect & Load All Tables"):
        try:
            url = URL.create(
                drivername="postgresql+psycopg2",
                username=db_user or None,
                password=db_password or None,
                host=db_host,
                port=int(db_port) if db_port else None,
                database=db_name,
            )
            engine = create_engine(url, pool_pre_ping=True)
            
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                    """)
                )
                table_names = [row[0] for row in result.fetchall()]
            
            for table in table_names:
                try:
                    all_data[table] = pd.read_sql(f'SELECT * FROM "{table}"', engine)
                    st.sidebar.success(f"✅ {table}: {len(all_data[table]):,} rows")
                except Exception as e:
                    st.sidebar.warning(f"⚠️ {table}: {str(e)}")
            
            st.session_state['all_data'] = all_data
            st.session_state['data_loaded'] = True
            
        except Exception as e:
            st.sidebar.error(f"❌ Connection failed: {str(e)}")
            st.stop()
    
    if st.session_state.get('data_loaded'):
        all_data = st.session_state.get('all_data', {})
    else:
        st.info("👈 Enter database credentials and click 'Connect & Load All Tables'")
        st.stop()

else:
    # CSV Files mode
    st.info("📁 Loading all CSV files from directory...")
    try:
        base_path = r"D:\DaiHoc\Kho du lieu\Dashboard"
        os.chdir(base_path)
        
        csv_files = ['Fact_AirQuality.csv', 'Fact_Forecast.csv', 'Dim_Time.csv', 'Dim_Date.csv', 
                     'Dim_Location.csv', 'Dim_Parameter.csv', 'Dim_Model.csv']
        
        for file in csv_files:
            try:
                all_data[file.replace('.csv', '')] = pd.read_csv(file, sep=";", encoding="ISO-8859-1")
                st.sidebar.success(f"✅ {file}: {len(all_data[file.replace('.csv', '')]):,} rows")
            except Exception as e:
                st.sidebar.warning(f"⚠️ {file}: {str(e)}")
        
        if not all_data:
            st.error("No data loaded. Please check CSV files in the directory.")
            st.stop()
            
    except Exception as e:
        st.error(f"Error loading CSV files: {str(e)}")
        st.stop()

# ========== MAIN ANALYSIS SECTION ==========
if not all_data:
    st.warning("No data available. Please load data first.")
    st.stop()

st.markdown("---")
st.header("📊 Comprehensive Multi-Table Analysis")
st.markdown(f"**Total Tables Loaded:** {len(all_data)}")

# Process each table
for table_name, df in all_data.items():
    st.markdown("---")
    st.markdown(f"## 📋 {table_name}")
    st.markdown(f"**Rows:** {len(df):,} | **Columns:** {len(df.columns)}")
    
    # Auto-detect columns
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    
    # Find date column
    date_column = date_cols[0] if date_cols else None
    if not date_column:
        potential_date_cols = [c for c in df.columns if 'date' in c.lower() or 'fulldate' in c.lower()]
        for col in potential_date_cols:
            parsed = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
            if parsed.notna().sum() > 0:
                df[col] = parsed
                date_column = col
                break
    
    # Auto-join dimensions if this is a fact table
    if any(key in df.columns for key in ['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey']):
        dimension_tables = {
            'DateKey': 'Dim_Date',
            'TimeKey': 'Dim_Time',
            'LocationKey': 'Dim_Location',
            'ParameterKey': 'Dim_Parameter',
            'ModelKey': 'Dim_Model'
        }
        
        for key_col, dim_table in dimension_tables.items():
            if key_col in df.columns and dim_table in all_data:
                try:
                    dim_df = all_data[dim_table].copy()
                    if dim_table == 'Dim_Date' and 'FullDate' in dim_df.columns:
                        dim_df['FullDate'] = pd.to_datetime(dim_df['FullDate'], errors='coerce')
                        if not date_column:
                            date_column = 'FullDate'
                    df = df.merge(dim_df, on=key_col, how='left', suffixes=('', f'_{dim_table}'))
                except Exception as e:
                    pass
    
    # Update column lists after joins
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    
    # Find value and category columns
    value_col = None
    if "Value" in numeric_cols:
        value_col = "Value"
    elif "Sales" in numeric_cols:
        value_col = "Sales"
    elif numeric_cols:
        value_col = [c for c in numeric_cols if c not in ['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey']]
        value_col = value_col[0] if value_col else None
    
    category_col = None
    suitable_cats = [c for c in categorical_cols if df[c].nunique() < 50 and df[c].nunique() > 1]
    category_col = suitable_cats[0] if suitable_cats else None
    
    # === VISUALIZATIONS ===
    col1, col2 = st.columns(2)
    
    # Chart 1: Bar chart
    if category_col:
        with col1:
            if value_col:
                st.subheader(f"{category_col} by {value_col}")
                chart_df = df.groupby(category_col)[value_col].sum().reset_index()
                fig = px.bar(chart_df, x=category_col, y=value_col, 
                            text=[f'{x:,.0f}' for x in chart_df[value_col]],
                            template="seaborn")
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.subheader(f"Distribution by {category_col}")
                chart_df = df[category_col].value_counts().reset_index()
                chart_df.columns = [category_col, 'Count']
                fig = px.bar(chart_df, x=category_col, y='Count', 
                            text=[f'{x:,}' for x in chart_df['Count']],
                            template="seaborn")
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    
        with col2:
            if value_col:
                st.subheader(f"{category_col} Distribution (Pie)")
                chart_df = df.groupby(category_col)[value_col].sum().reset_index()
                fig = px.pie(chart_df, values=value_col, names=category_col, hole=0.5)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.subheader(f"{category_col} Distribution (Pie)")
                chart_df = df[category_col].value_counts().reset_index()
                chart_df.columns = [category_col, 'Count']
                fig = px.pie(chart_df, values='Count', names=category_col, hole=0.5)
                st.plotly_chart(fig, use_container_width=True)
    
    # === SPECIALIZED ANALYTICS ===
    
    # Dim_Time specific analytics
    if 'Hour' in df.columns and 'Minute' in df.columns:
        st.markdown("### ⏱️ Time-Specific Analytics")
        tcol1, tcol2 = st.columns(2)
        
        with tcol1:
            hour_counts = df['Hour'].dropna().astype(int).value_counts().sort_index().reset_index()
            hour_counts.columns = ['Hour', 'Count']
            fig = px.bar(hour_counts, x='Hour', y='Count', title='Records by Hour', template='seaborn')
            st.plotly_chart(fig, use_container_width=True)
        
        with tcol2:
            fig = px.histogram(df.dropna(subset=['Minute']).astype({'Minute': 'int'}),
                             x='Minute', nbins=12, title='Minute Distribution', template='seaborn')
            st.plotly_chart(fig, use_container_width=True)
        
        # AM/PM analysis
        time_text_col = 'TimeStr' if 'TimeStr' in df.columns else ('TimeObj' if 'TimeObj' in df.columns else None)
        if time_text_col:
            tcol3, tcol4 = st.columns(2)
            texts = df[time_text_col].astype(str)
            period = np.where(texts.str.contains('SA', case=False, na=False), 'AM',
                            np.where(texts.str.contains('CH', case=False, na=False), 'PM', 'Unknown'))
            
            with tcol3:
                pie_df = pd.DataFrame({'Period': period})['Period'].value_counts().reset_index()
                pie_df.columns = ['Period', 'Count']
                fig = px.pie(pie_df, values='Count', names='Period', hole=0.5, title='AM vs PM Distribution')
                st.plotly_chart(fig, use_container_width=True)
            
            with tcol4:
                top_times = texts.value_counts().head(20).reset_index()
                top_times.columns = [time_text_col, 'Count']
                fig = px.bar(top_times, x=time_text_col, y='Count', title=f'Top 20 {time_text_col}', template='seaborn')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    
    # Fact_AirQuality analytics
    if 'Value' in df.columns and 'ParameterKey' in df.columns and 'LocationKey' in df.columns:
        st.markdown("### 🌫️ Air Quality Analytics")
        
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        
        # Parse dates/times
        if 'DateKey' in df.columns and 'DateParsed' not in df.columns:
            df['DateParsed'] = pd.to_datetime(df['DateKey'].astype(str), errors='coerce')
        
        if 'TimeKey' in df.columns and 'HourFromTimeKey' not in df.columns:
            tk = df['TimeKey'].astype(str).str.zfill(4)
            df['HourFromTimeKey'] = pd.to_numeric(tk.str.slice(0, 2), errors='coerce')
        
        aq1, aq2 = st.columns(2)
        
        with aq1:
            fig = px.histogram(df.dropna(subset=['Value']), x='Value', nbins=40,
                             title='Value Distribution', template='seaborn')
            st.plotly_chart(fig, use_container_width=True)
        
        with aq2:
            if 'HourFromTimeKey' in df.columns:
                hourly = df.groupby('HourFromTimeKey')['Value'].mean().reset_index()
                fig = px.line(hourly, x='HourFromTimeKey', y='Value',
                            title='Average Value by Hour', template='seaborn', markers=True)
                st.plotly_chart(fig, use_container_width=True)
        
        aq3, aq4 = st.columns(2)
        
        with aq3:
            if 'ParameterName' in df.columns:
                param_agg = df.groupby('ParameterName')['Value'].mean().reset_index()
                fig = px.bar(param_agg, x='ParameterName', y='Value',
                           title='Average Value by Parameter', template='seaborn')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        with aq4:
            if 'LocationName' in df.columns:
                loc_agg = df.groupby('LocationName')['Value'].mean().reset_index()
                fig = px.bar(loc_agg, x='LocationName', y='Value',
                           title='Average Value by Location', template='seaborn')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        # Time series
        if 'DateParsed' in df.columns:
            daily_agg = df.groupby('DateParsed')['Value'].mean().reset_index()
            fig = px.line(daily_agg, x='DateParsed', y='Value',
                        title='Air Quality Trend Over Time', template='seaborn', markers=True)
            st.plotly_chart(fig, use_container_width=True)
    
    # Fact_Forecast analytics
    if 'ForecastID' in df.columns and 'Value' in df.columns:
        st.markdown("### 🔮 Forecast Analytics")
        
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        
        if 'DateKey' in df.columns and 'DateParsed' not in df.columns:
            df['DateParsed'] = pd.to_datetime(df['DateKey'].astype(str), errors='coerce')
        
        if 'TimeKey' in df.columns and 'HourFromTimeKey' not in df.columns:
            tk = df['TimeKey'].astype(str).str.zfill(4)
            df['HourFromTimeKey'] = pd.to_numeric(tk.str.slice(0, 2), errors='coerce')
        
        fc1, fc2 = st.columns(2)
        
        with fc1:
            fig = px.histogram(df.dropna(subset=['Value']), x='Value', nbins=30,
                             title='Forecast Value Distribution', template='seaborn')
            st.plotly_chart(fig, use_container_width=True)
        
        with fc2:
            if 'HourFromTimeKey' in df.columns:
                hourly = df.groupby('HourFromTimeKey')['Value'].mean().reset_index()
                fig = px.line(hourly, x='HourFromTimeKey', y='Value',
                            title='Average Forecast by Hour', template='seaborn', markers=True)
                st.plotly_chart(fig, use_container_width=True)
        
        fc3, fc4 = st.columns(2)
        
        with fc3:
            if 'ParameterName' in df.columns:
                param_agg = df.groupby('ParameterName')['Value'].mean().reset_index()
                fig = px.bar(param_agg, x='ParameterName', y='Value',
                           title='Average Forecast by Parameter', template='seaborn')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        with fc4:
            if 'ModelName' in df.columns:
                model_agg = df.groupby('ModelName')['Value'].mean().reset_index()
                fig = px.bar(model_agg, x='ModelName', y='Value',
                           title='Average Forecast by Model', template='seaborn')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        
        if 'DateParsed' in df.columns:
            daily = df.groupby('DateParsed')['Value'].mean().reset_index()
            fig = px.line(daily, x='DateParsed', y='Value',
                        title='Forecast Trend Over Time', template='seaborn', markers=True)
            st.plotly_chart(fig, use_container_width=True)
    
    # Data preview
    with st.expander(f"📄 View {table_name} Data"):
        st.dataframe(df.head(100), use_container_width=True)
    
    # Download option
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label=f"📥 Download {table_name} as CSV",
        data=csv,
        file_name=f"{table_name}.csv",
        mime='text/csv',
    )

st.markdown("---")
st.success("✅ All tables analyzed successfully!")

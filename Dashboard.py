import streamlit as st
import plotly.express as px
import pandas as pd
import os
import warnings
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
warnings.filterwarnings('ignore')

st.set_page_config(page_title="Superstore!!!", page_icon=":bar_chart:",layout="wide")

st.title(" :bar_chart: Sample SuperStore EDA")
st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html=True)

# Sidebar for data source selection
st.sidebar.header("📊 Data Source")
data_source = st.sidebar.radio("Choose data source:", ["PostgreSQL Database", "Upload File"])

if data_source == "PostgreSQL Database":
    st.sidebar.subheader("🔐 Database Connection")

    # Database connection parameters
    db_host = st.sidebar.text_input("Host", value="aws-1-ap-southeast-1.pooler.supabase.com")
    db_port = st.sidebar.text_input("Port", value="5432")
    db_name = st.sidebar.text_input("Database", value="postgres")
    db_user = st.sidebar.text_input("Username", value="")
    db_password = st.sidebar.text_input("Password", type="password")

    connect_clicked = st.sidebar.button("Connect to Database")

    if connect_clicked:
        try:
            # Build a safe URL (handles special characters like @ in password)
            url = URL.create(
                drivername="postgresql+psycopg2",
                username=db_user or None,
                password=db_password or None,
                host=db_host,
                port=int(db_port) if db_port else None,
                database=db_name,
            )
            engine = create_engine(url, pool_pre_ping=True)

            # Lấy danh sách bảng trong schema public
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        ORDER BY table_name
                    """
                ))
                table_names = [row[0] for row in result.fetchall()]

            st.session_state['engine'] = engine
            st.session_state['table_names'] = table_names
            st.session_state['connected'] = True
            st.sidebar.success("✅ Connected! Chọn bảng bên dưới để load dữ liệu")
        except Exception as e:
            st.sidebar.error(f"❌ Connection failed: {str(e)}")
            st.stop()

    if not st.session_state.get('connected'):
        st.info("👈 Nhập thông tin kết nối và bấm 'Connect to Database'")
        st.stop()

    table_names = st.session_state.get('table_names', [])
    if not table_names:
        st.sidebar.warning("Không tìm thấy bảng nào trong schema public")
        st.stop()

    selected_table = st.sidebar.selectbox("Table Name", table_names, key="table_select")

    if st.sidebar.button("Load Table"):
        try:
            df = pd.read_sql(f'SELECT * FROM "{selected_table}"', st.session_state['engine'])
            st.session_state['df'] = df
            st.sidebar.success(f"Loaded {len(df)} rows from {selected_table}")
        except Exception as e:
            st.sidebar.error(f"Không load được bảng: {str(e)}")
            st.stop()

    if 'df' in st.session_state:
        df = st.session_state['df']
    else:
        st.info("👈 Chọn bảng và bấm 'Load Table'")
        st.stop()

else:
    # File upload option
    fl = st.file_uploader(":file_folder: Upload a file",type=(["csv","txt","xlsx","xls"]))
    if fl is not None:
        filename = fl.name
        st.write(filename)
        if filename.endswith('.csv'):
            df = pd.read_csv(fl, encoding = "ISO-8859-1")
        else:
            df = pd.read_excel(fl)
    else:
        try:
            os.chdir(r"D:\DaiHoc\Kho du lieu\Dashboard")
            df = pd.read_excel("Superstore.xlsx")
        except Exception as e:
            st.error(f"Lỗi khi đọc file Superstore.xlsx: {str(e)}")
            st.warning("⚠️ File Superstore.xlsx bị lỗi hoặc không hợp lệ. Vui lòng upload file dữ liệu của bạn bằng nút upload ở trên!")
            st.stop()

# ===== AUTO-DETECT COLUMNS =====
numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

# Find date column (if not already datetime)
date_column = None
if date_cols:
    date_column = date_cols[0]
else:
    for col in categorical_cols:
        try:
            pd.to_datetime(df[col])
            date_column = col
            df[col] = pd.to_datetime(df[col])
            break
        except:
            pass

# ===== AUTO-JOIN ALL DIMENSION TABLES =====
st.sidebar.markdown("---")
st.sidebar.subheader("🔗 Auto-Join Dimensions")

dimension_tables = {
    'DateKey': 'Dim_Date',
    'TimeKey': 'Dim_Time',
    'LocationKey': 'Dim_Location',
    'ParameterKey': 'Dim_Parameter',
    'ModelKey': 'Dim_Model'
}

joined_dims = []

for key_col, dim_table in dimension_tables.items():
    if key_col in df.columns:
        try:
            # Try to load dimension table from database or CSV
            if data_source == "PostgreSQL Database":
                try:
                    dim_df = pd.read_sql(f'SELECT * FROM "{dim_table}"', st.session_state['engine'])
                except:
                    # Try CSV as fallback
                    try:
                        os.chdir(r"D:\DaiHoc\Kho du lieu\Dashboard")
                        dim_df = pd.read_csv(f"{dim_table}.csv", sep=";")
                    except:
                        dim_df = None
            else:
                try:
                    os.chdir(r"D:\DaiHoc\Kho du lieu\Dashboard")
                    dim_df = pd.read_csv(f"{dim_table}.csv", sep=";")
                except:
                    dim_df = None
            
            if dim_df is not None:
                # Convert FullDate to datetime if it's Dim_Date
                if dim_table == 'Dim_Date' and 'FullDate' in dim_df.columns:
                    dim_df['FullDate'] = pd.to_datetime(dim_df['FullDate'], errors='coerce')
                    if not date_column:
                        date_column = 'FullDate'
                
                # Join with dimension table
                df = df.merge(dim_df, on=key_col, how='left', suffixes=('', f'_{dim_table}'))
                joined_dims.append(dim_table)
                
        except Exception as e:
            st.sidebar.warning(f"⚠️ {dim_table}: {str(e)}")

if joined_dims:
    st.sidebar.success(f"✅ Joined: {', '.join(joined_dims)}")

# Update column lists after joins
numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

# Re-check date column after joins
if not date_column and date_cols:
    date_column = date_cols[0]

# ===== DATE FILTER =====
if date_column and date_column in df.columns:
    # Check if date column has valid dates (not all NaT)
    valid_dates = df[date_column].dropna()
    if len(valid_dates) > 0:
        col1, col2 = st.columns((2))
        startDate = valid_dates.min()
        endDate = valid_dates.max()
        
        with col1:
            date1 = pd.to_datetime(st.date_input("Start Date", startDate))
        with col2:
            date2 = pd.to_datetime(st.date_input("End Date", endDate))
        
        df = df[(df[date_column] >= date1) & (df[date_column] <= date2)].copy()

# ===== CATEGORICAL FILTERS =====
st.sidebar.header("Choose your filter: ")
filter_state = {}

for cat_col in categorical_cols[:5]:  # Limit to 5 filters
    selected = st.sidebar.multiselect(f"Pick {cat_col}", df[cat_col].unique())
    filter_state[cat_col] = selected
    if selected:
        df = df[df[cat_col].isin(selected)]

filtered_df = df

# ===== AUTO VISUALIZATION =====
col1, col2 = st.columns((2))

# Find suitable columns for visualization
value_col = None
if "Sales" in numeric_cols:
    value_col = "Sales"
elif "Value" in numeric_cols:
    value_col = "Value"
elif numeric_cols:
    value_col = numeric_cols[0]

category_col = None
if "Category" in categorical_cols:
    category_col = "Category"
elif categorical_cols:
    category_col = categorical_cols[0]

region_col = None
if "Region" in categorical_cols:
    region_col = "Region"

# Chart 1: Category/Group wise Sum OR Count
if category_col:
    with col1:
        if value_col and value_col not in ['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey']:
            # Has value column - show sum
            st.subheader(f"{category_col} wise {value_col}")
            chart_df = filtered_df.groupby(by=[category_col], as_index=False)[value_col].sum()
            
            fig = px.bar(chart_df, x=category_col, y=value_col, 
                        text=[f'${x:,.2f}' if isinstance(x, (int, float)) else x for x in chart_df[value_col]],
                        template="seaborn")
            st.plotly_chart(fig, use_container_width=True)
        else:
            # No value column - show count distribution
            st.subheader(f"Distribution by {category_col}")
            chart_df = filtered_df[category_col].value_counts().reset_index()
            chart_df.columns = [category_col, 'Count']
            
            fig = px.bar(chart_df, x=category_col, y='Count', 
                        text=[f'{x:,}' for x in chart_df['Count']],
                        template="seaborn")
            st.plotly_chart(fig, use_container_width=True)

# Chart 2: Region/Group wise Pie OR Distribution
if region_col and value_col and value_col not in ['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey']:
    with col2:
        st.subheader(f"{region_col} wise {value_col}")
        fig = px.pie(filtered_df, values=value_col, names=region_col, hole=0.5)
        fig.update_traces(text=filtered_df[region_col], textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
elif category_col and value_col and value_col not in ['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey']:
    with col2:
        st.subheader(f"{category_col} wise {value_col} (Pie)")
        fig = px.pie(filtered_df, values=value_col, names=category_col, hole=0.5)
        st.plotly_chart(fig, use_container_width=True)
elif category_col:
    with col2:
        st.subheader(f"{category_col} Distribution (Pie)")
        chart_df = filtered_df[category_col].value_counts().reset_index()
        chart_df.columns = [category_col, 'Count']
        fig = px.pie(chart_df, values='Count', names=category_col, hole=0.5)
        st.plotly_chart(fig, use_container_width=True)

cl1, cl2 = st.columns((2))
with cl1:
    with st.expander("View Data Summary"):
        if category_col and value_col:
            summary_df = filtered_df.groupby(by=[category_col], as_index=False)[value_col].sum()
            st.write(summary_df.style.background_gradient(cmap="Blues"))
        else:
            st.write(filtered_df.head(10))

with cl2:
    with st.expander("Data Info"):
        st.write(f"**Rows:** {len(filtered_df)}")
        st.write(f"**Columns:** {', '.join(df.columns)}")
        st.write(f"**Data Types:**")
        st.write(df.dtypes)
st.divider()
st.markdown("### 📊 Advanced Analytics")

# Time Series (if date column exists)
if date_column:
    st.subheader('Time Series Analysis')
    if value_col:
        timeseries_df = filtered_df.groupby(filtered_df[date_column].dt.to_period("M"))[value_col].sum().reset_index()
        timeseries_df[date_column] = timeseries_df[date_column].astype(str)
        fig2 = px.line(timeseries_df, x=date_column, y=value_col, labels={value_col: "Amount"}, template="plotly_dark")
        fig2.update_layout(height=500)
        st.plotly_chart(fig2, use_container_width=True)

# Scatter Plot (if multiple numeric columns)
if len(numeric_cols) >= 2:
    st.subheader("Relationship Analysis")
    col_x = st.selectbox("X axis", numeric_cols, key="scatter_x")
    col_y = st.selectbox("Y axis", numeric_cols, key="scatter_y", index=min(1, len(numeric_cols)-1))
    
    if col_x != col_y:
        fig = px.scatter(filtered_df, x=col_x, y=col_y, 
                        title=f"{col_x} vs {col_y}")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# Download Data
st.markdown("### 📥 Download Data")
csv = filtered_df.to_csv(index=False).encode('utf-8')
st.download_button('Download Filtered Data', data=csv, file_name="filtered_data.csv", mime="text/csv")

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import warnings
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

warnings.filterwarnings('ignore')

# 1. CẤU HÌNH KẾT NỐI DB CỐ ĐỊNH
DB_CONFIG = {
    "host": "aws-1-ap-southeast-1.pooler.supabase.com",
    "port": "5432",
    "name": "postgres",
    "user": "postgres.bkqhsxdynslfdtkcucij",
    "pass": "Duy@12345"
}

# 2. ĐỊNH NGHĨA NGƯỠNG QUỐC TẾ
THRESHOLDS = {
    "pm25": {"safe": 15, "warn": 35, "danger": 150},
    "pm10": {"safe": 45, "warn": 100, "danger": 250},
    "no2": {"safe": 25, "warn": 50, "danger": 200},
    "so2": {"safe": 40, "warn": 200, "danger": 500},
    "o3": {"safe": 100, "warn": 180, "danger": 240},
    "co": {"safe": 4, "warn": 10, "danger": 30}
}

# 3. Cấu hình trang
st.set_page_config(page_title="AQI Monitoring & Forecast", layout="wide")
st.markdown("<style>.stButton button { width: 100%; border-radius: 5px; height: 2.2rem; font-weight: bold; } [data-testid='stSidebar'] { display: none; }</style>", unsafe_allow_html=True)

# 4. Hàm bổ trợ dữ liệu
def get_engine():
    u, p = quote_plus(DB_CONFIG["user"]), quote_plus(DB_CONFIG["pass"])
    return create_engine(f"postgresql+psycopg2://{u}:{p}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}", pool_pre_ping=True, pool_recycle=300)

def format_time_str(val):
    s = str(int(val)).zfill(4)
    return f"{s[:2]}:{s[2:]}"

@st.cache_data(ttl=10)
def load_air_data(start_dt, end_dt):
    try:
        engine = get_engine()
        s, e = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        with engine.connect() as conn:
            query = text(f"""
                SELECT f.*, d."FullDate", t."TimeStr", p."ParameterName", p."Unit" 
                FROM "Fact_AirQuality" f 
                JOIN "Dim_Date" d ON f."DateKey" = d."DateKey" 
                JOIN "Dim_Time" t ON f."TimeKey" = t."TimeKey" 
                JOIN "Dim_Parameter" p ON f."ParameterKey" = p."ParameterKey" 
                WHERE d."FullDate" BETWEEN '{s}' AND '{e}'
                ORDER BY d."FullDate" ASC, t."TimeStr" ASC
            """)
            df = pd.read_sql_query(query, conn)
        if df.empty: return pd.DataFrame()
        df['FullDateTime'] = pd.to_datetime(df['FullDate'].astype(str) + ' ' + df['TimeStr'].astype(str))
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        return df.dropna(subset=['Value', 'FullDateTime'])
    except: return pd.DataFrame()

@st.cache_data(ttl=10)
def load_forecast_data(start_dt, end_dt):
    try:
        engine = get_engine()
        s, e = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with engine.connect() as conn:
            # Chỉ lấy ModelKey = 2 và sau thời gian hiện tại
            query = text(f"""
                SELECT f.*, d."FullDate", t."TimeStr", p."ParameterName", p."Unit"
                FROM "Fact_Forecast" f
                JOIN "Dim_Date" d ON f."DateKey" = d."DateKey"
                JOIN "Dim_Time" t ON f."TimeKey" = t."TimeKey"
                JOIN "Dim_Parameter" p ON f."ParameterKey" = p."ParameterKey"
                WHERE f."ModelKey" = 2 
                AND d."FullDate" BETWEEN '{s}' AND '{e}'
                ORDER BY d."FullDate" ASC, t."TimeStr" ASC
            """)
            df = pd.read_sql_query(query, conn)
        if df.empty: return pd.DataFrame()
        
        df['FullDateTime'] = pd.to_datetime(df['FullDate'].astype(str) + ' ' + df['TimeStr'].astype(str))
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        
        # Lọc bỏ các dữ liệu dự báo cũ (chỉ lấy tương lai)
        df = df[df['FullDateTime'] > datetime.now()]
        return df.dropna(subset=['Value', 'FullDateTime'])
    except: return pd.DataFrame()

@st.cache_data(ttl=10)
def load_weather_data(start_dt, end_dt):
    try:
        engine = get_engine()
        s, e = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        with engine.connect() as conn:
            query = text(f'SELECT w.*, d."FullDate", t."TimeStr" FROM "Fact_Weather" w JOIN "Dim_Date" d ON w."DateKey" = d."DateKey" JOIN "Dim_Time" t ON w."TimeKey" = t."TimeKey" WHERE d."FullDate" BETWEEN \'{s}\' AND \'{e}\' ORDER BY d."FullDate" ASC, t."TimeStr" ASC')
            df = pd.read_sql_query(query, conn)
        if df.empty: return pd.DataFrame()
        for col in ["Temperature", "Humidity", "WindSpeed", "Rain", "Pressure", "CloudCover"]:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        df['FullDateTime'] = pd.to_datetime(df['FullDate'].astype(str) + ' ' + df['TimeStr'].astype(str))
        return df.dropna(subset=['FullDateTime'])
    except: return pd.DataFrame()

def get_status_info(param, value):
    p = param.lower()
    if p not in THRESHOLDS: return "N/A", "#a3a8b8", "rgba(163, 168, 184, 0.1)"
    conf = THRESHOLDS[p]
    if value <= conf["safe"]: return "TỐT", "#00d4ff", "rgba(0, 212, 255, 0.1)"
    if value <= conf["warn"]: return "TRUNG BÌNH", "#ffcc00", "rgba(255, 204, 0, 0.1)"
    return "NGUY HIỂM", "#FF0000", "rgba(255, 0, 0, 0.2)"

# 5. KHỞI TẠO SESSION STATE
today_date = date.today()
if 'start_date' not in st.session_state: st.session_state.start_date = today_date - timedelta(days=today_date.weekday())
if 'end_date' not in st.session_state: st.session_state.end_date = today_date + timedelta(days=2) # Mặc định xem cả dự báo 2 ngày tới

# 6. GIAO DIỆN BỘ LỌC
st.title("🌦️ AQI & Weather Monitoring Dashboard (Real-time & Forecast)")
st.markdown("---")
cf1, cf2 = st.columns([1, 2])
with cf1:
    d_input = st.date_input("📅 Khoảng thời gian:", value=(st.session_state.start_date, st.session_state.end_date))
    if isinstance(d_input, tuple) and len(d_input) == 2: st.session_state.start_date, st.session_state.end_date = d_input
    q1, q2, q3 = st.columns(3)
    if q1.button("Hôm nay"): st.session_state.start_date = st.session_state.end_date = today_date; st.rerun()
    if q2.button("Tuần này"): st.session_state.start_date = today_date - timedelta(days=today_date.weekday()); st.session_state.end_date = today_date; st.rerun()
    if q3.button("Dự báo"): st.session_state.start_date = today_date; st.session_state.end_date = today_date + timedelta(days=7); st.rerun()

tab_air, tab_weather = st.tabs(["💎 Chất lượng Không khí", "🌤️ Thông số Thời tiết"])

# Biến cột vàng
now_full = datetime.now().replace(minute=0, second=0, microsecond=0)
is_today_in_range = st.session_state.start_date <= today_date <= st.session_state.end_date

def add_now_line(fig):
    if is_today_in_range:
        fig.add_shape(type="line", x0=now_full, x1=now_full, y0=0, y1=1, xref="x", yref="paper", line=dict(color="yellow", width=3, dash="dash"))
        fig.add_annotation(x=now_full, y=1, text="Bây giờ", showarrow=False, font=dict(color="yellow", size=12), bgcolor="rgba(0,0,0,0.5)", yanchor="bottom")
    return fig

with tab_air:
    df_air = load_air_data(st.session_state.start_date, st.session_state.end_date)
    df_forecast = load_forecast_data(st.session_state.start_date, st.session_state.end_date)
    
    if not df_air.empty:
        s_params = st.multiselect("⚗️ Chọn loại khí:", options=sorted(df_air['ParameterName'].unique()), default=sorted(df_air['ParameterName'].unique())[:6])
        
        @st.fragment(run_every=300)
        def render_air():
            cols = st.columns(2)
            for i, param in enumerate(s_params):
                with cols[i % 2]:
                    # Lọc dữ liệu thực tế và dự báo cho từng loại khí
                    df_p_real = df_air[df_air['ParameterName'] == param].copy()
                    df_p_fore = df_forecast[df_forecast['ParameterName'] == param].copy() if not df_forecast.empty else pd.DataFrame()
                    
                    if not df_p_real.empty:
                        last_v = df_p_real['Value'].iloc[-1]
                        peak_pt = df_p_real.loc[[df_p_real['Value'].idxmax()]]
                        txt, clr, bg = get_status_info(param, last_v)
                        st.markdown(f'<div style="background-color: {bg}; border: 2px solid {clr}; padding:15px; border-radius:10px; margin-bottom:5px;"><b>{param.upper()} Hiện tại</b>: {last_v:,.2f} <span style="float:right; color:{clr};">{txt}</span></div>', unsafe_allow_html=True)
                        
                        fig = go.Figure()
                        # Vẽ đường Thực tế (Trắng liền)
                        fig.add_trace(go.Scatter(x=df_p_real['FullDateTime'], y=df_p_real['Value'], mode='lines', name='Thực tế', line=dict(color='white', width=2)))
                        
                        # Vẽ đường Dự báo (Cam đứt đoạn) - Chỉ nếu có dữ liệu tương lai
                        if not df_p_fore.empty:
                            # Nối điểm cuối của thực tế với điểm đầu của dự báo để biểu đồ liên tục
                            connect_df = pd.concat([df_p_real.tail(1), df_p_fore])
                            fig.add_trace(go.Scatter(x=connect_df['FullDateTime'], y=connect_df['Value'], mode='lines', name='Dự báo (Model)', line=dict(color='#FFA500', width=2, dash='dot')))

                        # Điểm đỉnh thực tế
                        fig.add_trace(go.Scatter(x=peak_pt['FullDateTime'], y=peak_pt['Value'], mode='markers', name='Đỉnh thực tế', marker=dict(color='#FF0000', size=12, symbol='circle', line=dict(color='white', width=1))))
                        
                        fig = add_now_line(fig)
                        conf = THRESHOLDS.get(param.lower(), {"safe": 15, "warn": 35, "danger": 150})
                        fig.add_hrect(y0=0, y1=conf["safe"], fillcolor="rgba(0, 212, 255, 0.05)", line_width=0)
                        fig.add_hrect(y0=conf["safe"], y1=conf["warn"], fillcolor="rgba(255, 204, 0, 0.05)", line_width=0)
                        fig.add_hrect(y0=conf["warn"], y1=max(df_p_real['Value'].max()*1.2, conf["danger"]), fillcolor="rgba(255, 0, 0, 0.15)", line_width=0)
                        fig.update_layout(height=320, template="plotly_dark", margin=dict(l=10, r=10, t=25, b=10), showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                        st.plotly_chart(fig, use_container_width=True, key=f"air_{param}")
        render_air()
    else: st.warning("Không có dữ liệu không khí.")

with tab_weather:
    df_weather_main = load_weather_data(st.session_state.start_date, st.session_state.end_date)
    if not df_weather_main.empty:
        @st.fragment(run_every=300)
        def render_weather():
            fig_th = go.Figure()
            fig_th.add_trace(go.Scatter(x=df_weather_main['FullDateTime'], y=df_weather_main['Temperature'], name="Nhiệt độ (°C)", line=dict(color='#FF4B4B', width=3)))
            fig_th.add_trace(go.Scatter(x=df_weather_main['FullDateTime'], y=df_weather_main['Humidity'], name="Độ ẩm (%)", line=dict(color='#00D4FF', width=3), yaxis="y2"))
            fig_th = add_now_line(fig_th)
            fig_th.update_layout(title="Tương quan Nhiệt độ & Độ ẩm", template="plotly_dark", hovermode="x unified", yaxis=dict(title=dict(text="Nhiệt độ (°C)", font=dict(color="#FF4B4B")), tickfont=dict(color="#FF4B4B")), yaxis2=dict(title=dict(text="Độ ẩm (%)", font=dict(color="#00D4FF")), tickfont=dict(color="#00D4FF"), overlaying="y", side="right"), height=450)
            st.plotly_chart(fig_th, use_container_width=True)
            
            c1, c2 = st.columns(2)
            with c1: 
                fig_rain = px.bar(df_weather_main, x='FullDateTime', y='Rain', title="Lượng mưa (mm)", template="plotly_dark")
                st.plotly_chart(add_now_line(fig_rain), use_container_width=True)
            with c2: 
                fig_wind = px.line(df_weather_main, x='FullDateTime', y='WindSpeed', title="Tốc độ gió (m/s)", template="plotly_dark")
                st.plotly_chart(add_now_line(fig_wind), use_container_width=True)
        render_weather()

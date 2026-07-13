import os
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# Set page config to wide layout and custom title
st.set_page_config(
    page_title="SmartLogix Live Shipment Dashboard",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Auto-refresh every 2 seconds
st_autorefresh(interval=2000, key="data_refresh")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "smartlogix.db")

def read_db_table(table_name):
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# Inject gorgeous dark mode custom CSS
st.markdown("""
<style>
    /* Styling for glassmorphic cards */
    .kpi-card {
        background: rgba(30, 34, 45, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 30px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(5px);
        -webkit-backdrop-filter: blur(5px);
        margin-bottom: 15px;
        transition: transform 0.2s ease-in-out;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
        border-color: rgba(255, 255, 255, 0.2);
    }
    .kpi-title {
        color: #8A94A6;
        font-size: 14px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 8px;
    }
    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        margin: 0;
        background: linear-gradient(135deg, #FF6B6B 0%, #FFD93D 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .kpi-value-blue {
        font-size: 32px;
        font-weight: 700;
        margin: 0;
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .kpi-value-green {
        font-size: 32px;
        font-weight: 700;
        margin: 0;
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .header-style {
        text-align: center;
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 42px;
        font-weight: 800;
        margin-top: 10px;
        margin-bottom: 5px;
    }
    .subheader-style {
        text-align: center;
        color: #8A94A6;
        font-size: 16px;
        margin-bottom: 30px;
    }
</style>
""", unsafe_allow_html=True)

# App Header
st.markdown('<div class="header-style">SmartLogix Live Shipment Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="subheader-style">Real-time PySpark & Kafka streaming dashboard of active shipments</div>', unsafe_allow_html=True)

# Fetch Data
kpis_df = read_db_table("kpis")
city_df = read_db_table("city_metrics")
status_df = read_db_table("status_metrics")
vehicle_df = read_db_table("vehicle_metrics")
high_priority_df = read_db_table("high_priority_metrics")
windowed_df = read_db_table("windowed_metrics")
shipments_df = read_db_table("shipments")

# Extract KPI values
total_shipments = int(kpis_df.loc[0, 'total_shipments']) if not kpis_df.empty else 0
total_revenue = float(kpis_df.loc[0, 'total_revenue']) if not kpis_df.empty and kpis_df.loc[0, 'total_revenue'] is not None else 0.0
avg_weight = float(kpis_df.loc[0, 'avg_weight']) if not kpis_df.empty and kpis_df.loc[0, 'avg_weight'] is not None else 0.0
unique_vehicles = int(vehicle_df.loc[0, 'unique_vehicles']) if not vehicle_df.empty else 0


# 1. Top KPI Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Active Shipments</div>
        <div class="kpi-value">{total_shipments:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Total Revenue</div>
        <div class="kpi-value-green">₹{total_revenue:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Avg Weight</div>
        <div class="kpi-value-blue">{avg_weight:,.2f} kg</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-title">Active Vehicles</div>
        <div class="kpi-value">{unique_vehicles}</div>
    </div>
    """, unsafe_allow_html=True)

# 2. Main Analytics Charts
c1, c2 = st.columns(2)

with c1:
    st.markdown("### 📍 Shipments by Destination City")
    if not city_df.empty:
        city_df = city_df.sort_values(by="shipment_count", ascending=True)
        fig_city = px.bar(
            city_df,
            x="shipment_count",
            y="destination",
            orientation="h",
            labels={"shipment_count": "Shipments", "destination": "City"},
            color="shipment_count",
            color_continuous_scale="Viridis",
            text="shipment_count"
        )
        fig_city.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#8A94A6',
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            coloraxis_showscale=False
        )
        fig_city.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
        fig_city.update_yaxes(showgrid=False)
        st.plotly_chart(fig_city, use_container_width=True)
    else:
        st.info("Waiting for city metrics data...")

with c2:
    st.markdown("### 📦 Shipment Status Breakdown")
    if not status_df.empty:
        fig_status = px.pie(
            status_df,
            values="count",
            names="status",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_status.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#8A94A6',
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
        )
        fig_status.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_status, use_container_width=True)
    else:
        st.info("Waiting for status metrics data...")

# 3. Challenge Section & Windowed Metrics
st.markdown("---")
challenge_col, window_col = st.columns(2)

with challenge_col:
    st.markdown("### 🔥 Challenge: High Priority Shipments Monitor")
    if not high_priority_df.empty:
        high_priority_df = high_priority_df.sort_values(by="shipment_count", ascending=False)
        fig_priority = px.bar(
            high_priority_df,
            x="destination",
            y="shipment_count",
            labels={"shipment_count": "High-Priority Count", "destination": "City"},
            color="avg_weight",
            color_continuous_scale="Reds",
            title="City-wise High-Priority Count & Avg Weight (Color Scale)",
            text="shipment_count"
        )
        fig_priority.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font_color='#8A94A6',
            height=300,
            margin=dict(l=10, r=10, t=40, b=10)
        )
        fig_priority.update_xaxes(showgrid=False)
        fig_priority.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
        st.plotly_chart(fig_priority, use_container_width=True)
    else:
        st.info("No high-priority shipments recorded yet.")

with window_col:
    st.markdown("### ⏳ Live 10s Window Analytics (Tumbling)")
    if not windowed_df.empty:
        windowed_df["window_time"] = windowed_df["window_start"].str.slice(11, 19) + " - " + windowed_df["window_end"].str.slice(11, 19)
        latest_window_df = windowed_df.sort_values(by="window_end", ascending=False).head(10)
        st.dataframe(
            latest_window_df[["window_time", "destination", "shipment_count", "avg_weight"]],
            column_config={
                "window_time": "Time Window",
                "destination": "City",
                "shipment_count": "Shipments",
                "avg_weight": "Avg Weight (kg)"
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("Waiting for windowed metrics to accumulate...")

# 4. Live Log
st.markdown("---")
st.markdown("### 📋 Live Shipment Event Log (Recent 15)")
if not shipments_df.empty:
    recent_shipments = shipments_df.sort_values(by="timestamp", ascending=False).head(15)
    st.dataframe(
        recent_shipments[["timestamp", "shipment_id", "priority", "origin", "destination", "vehicle_id", "weight", "status", "revenue"]],
        column_config={
            "timestamp": "Timestamp",
            "shipment_id": "Shipment ID",
            "priority": "Priority",
            "origin": "Origin",
            "destination": "Destination",
            "vehicle_id": "Vehicle ID",
            "weight": "Weight (kg)",
            "status": "Status",
            "revenue": "Revenue (₹)"
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("Waiting for shipment events...")

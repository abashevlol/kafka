import streamlit as st
import json
from streamlit_autorefresh import st_autorefresh

# Optional: auto-refresh every 5 seconds
st_autorefresh(interval=5000, key="alert_count_refresh")

st.title("Alert Count KPI")

# Read latest KPI from file (replace with your actual data source)
try:
    with open("latest_alert_count.json") as f:
        data = json.load(f)
    count = data["count"]
    window_start = data["window_start"]
    window_end = data["window_end"]
except Exception as e:
    count = "N/A"
    window_start = "N/A"
    window_end = "N/A"
    st.warning(f"Could not load KPI: {e}")

st.metric(
    label=f"Alerts in last 5 seconds\n({window_start} to {window_end})",
    value=count
)

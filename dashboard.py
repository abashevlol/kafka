import streamlit as st
import json
from collections import deque
from datetime import datetime

# Initialize buffers for real-time chart
temperature_buffer = deque(maxlen=100)
timestamp_buffer = deque(maxlen=100)

st.title("Real-Time IoT Dashboard")

# --- KPI SECTION: Average Temperature (last 10s) ---
st.subheader("📊 Key Performance Indicators")
col1, col2 = st.columns([1, 2])

with col1:
    try:
        with open("latest_avg_temp.json") as f:
            data = json.load(f)
        avg_temp = data["avg_temp"]
        window_start = data["window_start"]
        window_end = data["window_end"]
    except Exception as e:
        avg_temp = "N/A"
        window_start = "N/A"
        window_end = "N/A"
        st.warning(f"Could not load KPI: {e}")
    
    st.metric(
        label="Average Temperature (10s)",
        value=f"{avg_temp} °C",
        help=f"From {window_start} to {window_end}"
    )

# --- REAL-TIME SECTION ---
st.divider()
st.subheader("📈 Real-Time Monitoring")

# Placeholders for dynamic content
current_temp_placeholder = st.empty()
chart_placeholder = st.empty()

try:
    from quixstreams import Application
    
    # Kafka connection setup
    @st.cache_resource
    def kafka_connection():
        return Application(
            broker_address="localhost:9092",
            consumer_group="dashboard",
            auto_offset_reset="latest",
        )

    app = kafka_connection()
    sensor_topic = app.topic("sensor")
    alert_topic = app.topic("alert")

    # Kafka consumer loop
    with app.get_consumer() as consumer:
        consumer.subscribe([sensor_topic.name, alert_topic.name])
        previous_temp = 0
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is not None and msg.topic() == sensor_topic.name:
                sensor_msg = sensor_topic.deserialize(msg)
                temperature = sensor_msg.value.get('temperature')
                device_id = sensor_msg.value.get('device_id')
                timestamp = datetime.fromisoformat(sensor_msg.value.get('timestamp'))
                diff = temperature - previous_temp
                previous_temp = temperature
                timestamp_str = timestamp.strftime("%H:%M:%S")
                
                # Update current temperature display
                current_temp_placeholder.metric(
                    label=f"Current Temperature ({device_id})",
                    value=f"{temperature:.2f} °C",
                    delta=f"{diff:.2f} °C"
                )
                
                # Update chart
                timestamp_buffer.append(timestamp_str)
                temperature_buffer.append(temperature)
                chart_placeholder.line_chart(
                    data={
                        "time": list(timestamp_buffer),
                        "temperature": list(temperature_buffer)
                    },
                    x="time",
                    y="temperature",
                    use_container_width=True,
                    height=300,
                )

except ModuleNotFoundError:
    st.error("QuixStreams not installed - real-time updates disabled")
    st.warning("Please install with: `pip install quixstreams`")
    
    # Show static example if QuixStreams not available
    current_temp_placeholder.metric(
        label="Current Temperature (simulated)",
        value="23.45 °C",
        delta="+0.12 °C"
    )
    chart_placeholder.line_chart(
        data={"time": ["10:00", "10:01", "10:02"], "temperature": [22.1, 23.4, 22.8]},
        x="time",
        y="temperature",
        height=300,
    )

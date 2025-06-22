Real-Time IoT Dashboard Explanation
This project demonstrates a real-time IoT dashboard built with Python, Kafka, Quix Streams, and Streamlit. The system uses a producer to simulate sensor data, a consumer to process and filter alerts, and a windowed consumer to calculate the average temperature over the last 10 seconds. The results are visualized in a Streamlit dashboard.

How It Works
Producer: Simulates IoT sensor data and sends it to the Kafka sensor topic.

Consumer: Processes sensor data, filters for alerts, and writes them to the alert topic.

Windowed Consumer: Calculates the average temperature in the last 10 seconds and writes the KPI to a file and a Kafka topic.

Streamlit Dashboard: Reads the latest KPI and displays it as a metric widget, along with real-time temperature updates and a line chart.
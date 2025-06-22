import logging
import json
from datetime import timedelta, datetime
from quixstreams import Application
from quixstreams.dataframe.windows import Mean

def custom_ts_extractor(value, headers, timestamp, timestamp_type):
    dt = datetime.fromisoformat(value["timestamp"])
    return int(dt.timestamp() * 1000)

def save_latest_kpi(value):
    """Save the latest average temperature KPI to a file for Streamlit to read"""
    result = {
        "avg_temp": round(value["avg_temp"], 2),
        "window_start": datetime.fromtimestamp(value["start"] / 1000).isoformat(),
        "window_end": datetime.fromtimestamp(value["end"] / 1000).isoformat()
    }
    
    # Write to file for Streamlit dashboard
    with open("latest_avg_temp.json", "w") as f:
        json.dump(result, f)
    
    logging.info(f"Saved latest KPI: {result}")
    return result

def main():
    logging.info("START AVG TEMP PIPELINE...")
    app = Application(
        broker_address="localhost:9092",
        consumer_group="avg-temp",
        auto_offset_reset="earliest",
    )

    input_topic = app.topic(
        "sensor",
        value_deserializer="json",
        timestamp_extractor=custom_ts_extractor
    )
    output_topic = app.topic("avg-temp-10s", value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Tumbling window of 10 seconds, averaging temperature
    sdf = (
        sdf.tumbling_window(timedelta(seconds=10))
        .agg(avg_temp=Mean("temperature"))
        .final()
    )

    # Save latest KPI to file AND format for Kafka topic
    sdf = sdf.apply(save_latest_kpi)

    sdf.to_topic(output_topic)
    app.run(sdf)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()

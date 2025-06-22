import logging
from datetime import timedelta, datetime
from quixstreams import Application
from quixstreams.dataframe.windows import Count

def custom_ts_extractor(value, headers, timestamp, timestamp_type):
    # Extract the event time from the alert message's ISO timestamp field
    dt = datetime.fromisoformat(value["timestamp"])
    return int(dt.timestamp() * 1000)  # milliseconds since epoch

def main():
    logging.info("START...")
    app = Application(
        broker_address="localhost:9092",
        consumer_group="alert-count",
        auto_offset_reset="earliest",  # Read all available alerts for testing
    )

    # Read from the 'alert' topic, use custom timestamp extractor for windowing
    input_topic = app.topic(
        "alert",
        value_deserializer="json",
        timestamp_extractor=custom_ts_extractor
    )
    output_topic = app.topic("alert-count-5s", value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Tumbling window of 5 seconds, counting alert events
    sdf = (
        sdf.tumbling_window(timedelta(seconds=5))
        .agg(count=Count())
        .final()  # Emit only when window closes
    )

    # Format output: convert window start/end to ISO strings for readability
    sdf = sdf.apply(lambda value: {
        "count": value["count"],
        "window_start": datetime.fromtimestamp(value["start"] / 1000).isoformat(),
        "window_end": datetime.fromtimestamp(value["end"] / 1000).isoformat()
    })

    sdf.to_topic(output_topic)
    app.run(sdf)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()

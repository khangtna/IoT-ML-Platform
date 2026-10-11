"""Persist raw events, then recompute affected windows across micro-batches."""
import argparse
import json
import os
import time
from datetime import timedelta
from pyspark.sql import SparkSession, functions as F
from common import read_table, upsert
from kafka_consumer import read_events
from transform import normalize, interpolate
from anomaly_detection import add_anomaly_features


def write_batch(raw, batch_id):
    started = time.monotonic()
    df = normalize(raw).cache()
    try:
        first, last, count = df.agg(F.min("timestamp"), F.max("timestamp"), F.count("*")).first()
        if not count:
            return
        upsert(df, "sensor_raw", ["timestamp", "station_id"])
        # Late observations can repair interpolation and subsequent baselines.
        gap = int(os.getenv("INTERPOLATION_MAX_GAP_SECONDS", "10800"))
        begin = first - timedelta(seconds=gap + 1800)
        end = last + timedelta(seconds=gap + 1800)
        lower = begin - timedelta(seconds=gap + 1800)
        upper = end + timedelta(seconds=gap + 1800)
        source = read_table(df.sparkSession,
            f"(SELECT * FROM sensor_raw WHERE timestamp >= '{lower.isoformat()}' "
            f"AND timestamp <= '{upper.isoformat()}') context")
        processed = add_anomaly_features(interpolate(source, gap)).cache()
        try:
            tumbling = (processed.groupBy("station_id", F.window("timestamp", "5 minutes"))
                        .agg(F.avg("pm25").alias("pm25_avg_5m")))
            output = (processed.withColumn("window", F.window("timestamp", "5 minutes"))
                .join(tumbling, ["station_id", "window"]).drop("window", "event_id")
                .filter(F.col("timestamp").between(begin, end)))
            upsert(output, "sensor_metrics", ["timestamp", "station_id"])
            for duration, label in [("5 minutes", "tumbling_5m"), ("30 minutes", "sliding_30m")]:
                windows = (processed.groupBy("station_id", F.window("timestamp", duration, "5 minutes"))
                    .agg(F.avg("pm25").alias("pm25_avg"), F.count("pm25").alias("sample_count"))
                    .select("station_id", F.col("window.start").alias("window_start"),
                            F.col("window.end").alias("window_end"), "pm25_avg", "sample_count")
                    .withColumn("window_type", F.lit(label))
                    .filter((F.col("window_end") > F.lit(begin)) & (F.col("window_start") <= F.lit(end))))
                upsert(windows, "sensor_windows", ["window_start", "station_id", "window_type"])
        finally:
            processed.unpersist()
        seconds = time.monotonic() - started
        print(json.dumps({"batch_id": batch_id, "valid_events": count,
            "processing_seconds": seconds, "processed_events_per_second": count / seconds}), flush=True)
    finally:
        df.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--available-now", action="store_true", help="Drain current Kafka backlog and exit")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("smart-city-streaming").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    writer = (read_events(spark).writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT_LOCATION", "/opt/checkpoints/smart-city-v2")))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="10 seconds")
    try:
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

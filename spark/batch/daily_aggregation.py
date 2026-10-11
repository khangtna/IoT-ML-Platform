"""Idempotent daily aggregates over an explicit UTC interval."""
import argparse
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, functions as F
from common import read_table, upsert


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00")) if args.end else datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")) if args.start else end - timedelta(days=1)
    if start >= end:
        raise ValueError("start must precede end")
    spark = SparkSession.builder.appName("daily-environment-summary").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        df = read_table(spark, "sensor_metrics").filter((F.col("timestamp") >= F.lit(start)) & (F.col("timestamp") < F.lit(end)))
        result = (df.groupBy(F.to_date("timestamp").alias("day"), "station_id")
            .agg(F.count("*").alias("event_count"), F.count("pm25").alias("valid_pm25_count"),
                 F.avg("pm25").alias("pm25_avg"), F.max("pm25").alias("pm25_max"),
                 F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
                 F.sum(F.col("is_threshold_exceeded").cast("int")).alias("threshold_count")))
        upsert(result, "daily_summary", ["day", "station_id"])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Trailing 30-minute baseline excludes the observation being tested."""
import os
from pyspark.sql import functions as F, Window


def add_anomaly_features(df):
    w = Window.partitionBy("station_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-1800, -1)
    df = (df.withColumn("pm25_rolling_mean_30m", F.avg("pm25").over(w))
          .withColumn("pm25_rolling_std_30m", F.stddev_pop("pm25").over(w))
          .withColumn("_count", F.count("pm25").over(w)))
    enough = F.col("_count") >= int(os.getenv("ANOMALY_MIN_SAMPLES", "3"))
    std = F.col("pm25_rolling_std_30m")
    df = df.withColumn("pm25_z_score", F.when(enough & (std > 0),
        (F.col("pm25") - F.col("pm25_rolling_mean_30m")) / std))
    # Constant baseline: undefined Z-score; the absolute threshold still applies.
    return (df.withColumn("is_anomaly", F.coalesce(F.abs(F.col("pm25_z_score")) >
        float(os.getenv("ANOMALY_Z_THRESHOLD", "3")), F.lit(False)))
        .withColumn("is_threshold_exceeded", F.coalesce(F.col("pm25") >
        float(os.getenv("PM25_ALERT_THRESHOLD", "75")), F.lit(False))).drop("_count"))

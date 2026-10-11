"""Hourly features use exact timestamp joins, never row offsets across gaps."""
from pyspark.sql import functions as F, Window

FEATURE_COLUMNS = ["pm25", "pm25_lag_1", "pm25_lag_2", "pm25_lag_3", "hour_of_day", "day_of_week"]


def build_feature_frame(df, as_of=None):
    # Keep measured observations for backtesting: bidirectional interpolation may
    # have used future points, so those values must not leak into ML evaluation.
    if "pm25_imputed" in df.columns:
        df = df.filter(~F.coalesce(F.col("pm25_imputed"), F.lit(False)))
    latest = (df.sparkSession.range(1).select(F.lit(as_of).alias("last_observed"))
              if as_of is not None else df.agg(F.max("timestamp").alias("last_observed")))
    hourly = (df.groupBy("station_id", F.window("timestamp", "1 hour"))
        .agg(F.avg("pm25").alias("pm25"))
        .select("station_id", F.col("window.end").alias("timestamp"), "pm25")
        .crossJoin(latest).filter(F.col("timestamp") <= F.col("last_observed")).drop("last_observed"))
    features = hourly
    for hours in (1, 2, 3):
        previous = hourly.select("station_id", (F.col("timestamp") + F.expr(f"INTERVAL {hours} HOURS")).alias("timestamp"),
                                 F.col("pm25").alias(f"pm25_lag_{hours}"))
        features = features.join(previous, ["station_id", "timestamp"], "left")
    return (features.withColumn("hour_of_day", F.hour("timestamp").cast("double"))
        .withColumn("day_of_week", F.dayofweek("timestamp").cast("double"))
        .dropna(subset=FEATURE_COLUMNS))


def build_training_frame(features, horizon_hours=1):
    labels = features.select("station_id", F.col("timestamp").alias("label_time"),
        (F.col("timestamp") - F.expr(f"INTERVAL {int(horizon_hours)} HOURS")).alias("timestamp"),
        F.col("pm25").alias("label"))
    return features.join(labels, ["station_id", "timestamp"])


def temporal_split(data, fraction=0.8):
    times = data.select(F.col("timestamp").cast("long").alias("t")).distinct()
    cutoff = times.approxQuantile("t", [fraction], 0.0)[0]
    # Training targets end before the holdout feature times begin.
    train = data.filter(F.col("label_time").cast("long") <= cutoff)
    test = data.filter(F.col("timestamp").cast("long") > cutoff)
    return train, test


def latest_features(features):
    w = Window.partitionBy("station_id").orderBy(F.col("timestamp").desc())
    return features.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")

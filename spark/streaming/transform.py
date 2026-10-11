"""Clean invalid readings and interpolate bounded gaps using event time."""
from pyspark.sql import functions as F, Window

NONNEGATIVE = ["pm25", "pm10", "so2", "no2", "co", "o3", "rainfall", "wind_speed"]
NUMERIC = NONNEGATIVE + ["temperature", "pressure", "dew_point"]


def normalize(df):
    df = df.withColumn("timestamp", F.to_timestamp("timestamp")).withColumn("station_id", F.trim("station_id"))
    df = df.filter(F.col("timestamp").isNotNull() & (F.length("station_id") > 0))
    for name in NUMERIC:
        valid = ~F.isnan(name) & (F.abs(F.col(name)) != float("inf"))
        if name in NONNEGATIVE:
            valid = valid & (F.col(name) >= 0)
        df = df.withColumn(name, F.when(valid, F.col(name)))
    w = Window.partitionBy("station_id", "timestamp").orderBy(F.col("event_id").desc_nulls_last())
    return df.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")


def interpolate(df, max_gap_seconds=10800):
    df = df.withColumn("_seconds", F.col("timestamp").cast("long"))
    past = Window.partitionBy("station_id").orderBy("_seconds").rangeBetween(-max_gap_seconds, 0)
    future = Window.partitionBy("station_id").orderBy("_seconds").rangeBetween(0, max_gap_seconds)
    df = df.withColumn("pm25_imputed", F.col("pm25").isNull())
    for name in NUMERIC:
        point = F.when(F.col(name).isNotNull(), F.struct(F.col("_seconds").alias("t"), F.col(name).alias("v")))
        df = df.withColumn("_before", F.last(point, ignorenulls=True).over(past))
        df = df.withColumn("_after", F.first(point, ignorenulls=True).over(future))
        distance = F.col("_after.t") - F.col("_before.t")
        estimate = F.col("_before.v") + (F.col("_after.v") - F.col("_before.v")) * (F.col("_seconds") - F.col("_before.t")) / distance
        df = df.withColumn(name, F.coalesce(F.col(name), F.when((distance > 0) & (distance <= max_gap_seconds), estimate)))
    return df.drop("_seconds", "_before", "_after").withColumn(
        "pm25_imputed", F.col("pm25_imputed") & F.col("pm25").isNotNull())

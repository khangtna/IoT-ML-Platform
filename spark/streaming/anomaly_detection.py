import os
from pyspark.sql import functions as F
def add_anomaly_features(df):
 th=float(os.getenv("ANOMALY_Z_THRESHOLD","3.0")); stats=df.groupBy("station_id").agg(F.avg("pm25").alias("pm25_rolling_mean_30m"),F.stddev_pop("pm25").alias("pm25_rolling_std_30m"))
 return (df.join(stats,"station_id","left").withColumn("pm25_z_score",F.when(F.col("pm25_rolling_std_30m")>0,(F.col("pm25")-F.col("pm25_rolling_mean_30m"))/F.col("pm25_rolling_std_30m"))).withColumn("is_anomaly",F.coalesce(F.abs(F.col("pm25_z_score"))>F.lit(th),F.lit(False))))

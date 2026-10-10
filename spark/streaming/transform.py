from pyspark.sql import functions as F
def normalize(df):
 return (df.withColumn("event_time",F.to_timestamp("timestamp")).drop("timestamp").withColumnRenamed("event_time","timestamp").filter(F.col("station_id").isNotNull()).filter(F.col("timestamp").isNotNull()).filter(F.col("pm25").isNotNull()).filter(F.col("pm25")>=0))

import os
from pyspark.sql import SparkSession,functions as F
from kafka_consumer import read_events
from transform import normalize
from anomaly_detection import add_anomaly_features
URL=os.getenv("JDBC_URL","jdbc:postgresql://timescaledb:5432/smartcity"); USER=os.getenv("POSTGRES_USER","smartcity"); PW=os.getenv("POSTGRES_PASSWORD","smartcity")
def write_batch(df,bid):
 if df.rdd.isEmpty(): return
 avg=(df.groupBy(F.window("timestamp","5 minutes"),"station_id").agg(F.avg("pm25").alias("pm25_avg_5m")).select("station_id",F.col("window.start").alias("window_start"),"pm25_avg_5m"))
 out=(add_anomaly_features(df).withColumn("window_start",F.window("timestamp","5 minutes").getField("start")).join(avg,["station_id","window_start"],"left").drop("window_start","event_id"))
 (out.write.format("jdbc").mode("append").option("url",URL).option("dbtable","sensor_metrics").option("user",USER).option("password",PW).option("driver","org.postgresql.Driver").save())
def main():
 s=(SparkSession.builder.appName("smart-city-streaming").config("spark.sql.session.timeZone","UTC").getOrCreate()); s.sparkContext.setLogLevel("WARN")
 q=(normalize(read_events(s)).withWatermark("timestamp","10 minutes").writeStream.foreachBatch(write_batch).option("checkpointLocation","/opt/checkpoints/smart-city-streaming").start()); q.awaitTermination()
if __name__=="__main__": main()

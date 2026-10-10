import os
from pyspark.sql.functions import col,from_json
from schemas import EVENT_SCHEMA
def read_events(spark):
 return (spark.readStream.format("kafka").option("kafka.bootstrap.servers",os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")).option("subscribe",os.getenv("KAFKA_TOPIC","sensor-events")).option("startingOffsets","latest").load().selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"),EVENT_SCHEMA).alias("event")).select("event.*"))

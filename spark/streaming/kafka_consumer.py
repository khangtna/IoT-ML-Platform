import os
from pyspark.sql import functions as F
from schemas import EVENT_SCHEMA


def read_events(spark):
    return (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "sensor-events"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))
        .load().select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*"))

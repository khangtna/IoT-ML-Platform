import os
def read_sensor_metrics(spark):
 return (spark.read.format("jdbc").option("url",os.getenv("JDBC_URL","jdbc:postgresql://timescaledb:5432/smartcity")).option("dbtable","sensor_metrics").option("user",os.getenv("POSTGRES_USER","smartcity")).option("password",os.getenv("POSTGRES_PASSWORD","smartcity")).option("driver","org.postgresql.Driver").load())

from pyspark.sql import functions as F
from pyspark.sql.window import Window
FEATURE_COLUMNS=["pm25","pm10","co","no2","temperature","pressure","dew_point","wind_speed","hour_of_day","day_of_week","pm25_lag_1","pm25_lag_2","pm25_lag_3"]
def build_training_frame(df,horizon_hours=1):
 w=Window.partitionBy("station_id").orderBy("timestamp")
 return (df.withColumn("hour_of_day",F.hour("timestamp").cast("double")).withColumn("day_of_week",F.dayofweek("timestamp").cast("double")).withColumn("pm25_lag_1",F.lag("pm25",1).over(w)).withColumn("pm25_lag_2",F.lag("pm25",2).over(w)).withColumn("pm25_lag_3",F.lag("pm25",3).over(w)).withColumn("label",F.lead("pm25",int(horizon_hours)).over(w)).dropna(subset=FEATURE_COLUMNS+["label"]))

import json,os
from datetime import datetime,timezone
from pyspark.sql import SparkSession,functions as F
from extract import read_sensor_metrics
from features import build_training_frame
from train_linear_regression import train as lr
from train_random_forest import train as rf
from evaluate import evaluate
URL=os.getenv("JDBC_URL","jdbc:postgresql://timescaledb:5432/smartcity"); USER=os.getenv("POSTGRES_USER","smartcity"); PW=os.getenv("POSTGRES_PASSWORD","smartcity")
def write_forecast(df,name,h):
 out=(df.select("timestamp","station_id","prediction").withColumn("created_at",F.lit(datetime.now(timezone.utc))).withColumn("forecast_time",F.col("timestamp")+F.expr(f"INTERVAL {int(h)} HOURS")).withColumn("horizon_hours",F.lit(int(h))).withColumnRenamed("prediction","predicted_pm25").withColumn("model_name",F.lit(name)).select("created_at","forecast_time","station_id","horizon_hours","predicted_pm25","model_name"))
 out.write.format("jdbc").mode("append").option("url",URL).option("dbtable","pm25_forecast").option("user",USER).option("password",PW).option("driver","org.postgresql.Driver").save()
def main():
 s=SparkSession.builder.appName("pm25-forecast-training").config("spark.sql.session.timeZone","UTC").getOrCreate(); src=read_sensor_metrics(s); data=build_training_frame(src,1); tr,te=data.randomSplit([0.8,0.2],seed=42)
 models={"linear_regression":lr(tr),"random_forest":rf(tr)}; res={}; preds={}
 for n,m in models.items(): preds[n]=m.transform(te); res[n]=evaluate(preds[n])
 best=min(res,key=lambda n:res[n]["rmse"]); print(json.dumps({"metrics":res,"best_model":best},indent=2)); write_forecast(preds[best],best,1); s.stop()
if __name__=="__main__": main()

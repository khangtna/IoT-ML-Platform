from pyspark.ml.evaluation import RegressionEvaluator
def evaluate(df): return {m:RegressionEvaluator(labelCol="label",predictionCol="prediction",metricName=m).evaluate(df) for m in ("mae","rmse","r2")}

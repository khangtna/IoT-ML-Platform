from pyspark.ml.evaluation import RegressionEvaluator


# So sánh prediction với label: MAE/RMSE càng thấp càng tốt, R² càng gần 1 càng tốt.
def evaluate(df):
    return {
        m: RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName=m,
        ).evaluate(df)
        for m in ("mae", "rmse", "r2")
    }

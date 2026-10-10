from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler,Imputer
from pyspark.ml.regression import LinearRegression
from features import FEATURE_COLUMNS
def train(df):
 imp=[f"{c}_imputed" for c in FEATURE_COLUMNS]
 return Pipeline(stages=[Imputer(inputCols=FEATURE_COLUMNS,outputCols=imp),VectorAssembler(inputCols=imp,outputCol="features"),LinearRegression(featuresCol="features",labelCol="label",predictionCol="prediction")]).fit(df)

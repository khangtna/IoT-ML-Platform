from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.regression import RandomForestRegressor

from features import FEATURE_COLUMNS


# Fit pipeline trên dữ liệu đầu vào; khi đánh giá, chỉ truyền tập train.
def train(df):
    # Tạo tên các cột đặc trưng sau khi điền giá trị thiếu.
    imp = [f"{c}_imputed" for c in FEATURE_COLUMNS]
    # Imputer điền thiếu bằng trung bình; VectorAssembler gom các cột thành vector.
    # Random Forest dùng 50 cây; seed cố định giúp tái lập kết quả.
    # label là giá trị thực cần học; prediction là giá trị mô hình dự báo.
    return Pipeline(
        stages=[
            Imputer(inputCols=FEATURE_COLUMNS, outputCols=imp),
            VectorAssembler(inputCols=imp, outputCol="features"),
            RandomForestRegressor(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                numTrees=50,
                seed=42,
            ),
        ]
    ).fit(df)

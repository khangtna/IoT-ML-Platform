"""Hourly features use exact timestamp joins, never row offsets across gaps."""

from pyspark.sql import functions as F, Window

# Đầu vào mô hình: PM2.5 hiện tại, ba giờ trước, giờ trong ngày và thứ trong tuần.
FEATURE_COLUMNS = [
    "pm25",
    "pm25_lag_1",
    "pm25_lag_2",
    "pm25_lag_3",
    "hour_of_day",
    "day_of_week",
]


def build_feature_frame(df, as_of=None):
    # Loại dữ liệu đã nội suy vì có thể dùng điểm tương lai, làm sai lệch đánh giá.
    if "pm25_imputed" in df.columns:
        df = df.filter(~F.coalesce(F.col("pm25_imputed"), F.lit(False)))
    # Chỉ giữ cửa sổ giờ đã kết thúc tại mốc dữ liệu cho phép.
    latest = (
        df.sparkSession.range(1).select(F.lit(as_of).alias("last_observed"))
        if as_of is not None
        else df.agg(F.max("timestamp").alias("last_observed"))
    )

    # Gom dữ liệu theo trạm và giờ; dùng thời điểm kết thúc cửa sổ làm mốc.
    hourly = (
        df.groupBy("station_id", F.window("timestamp", "1 hour"))
        .agg(F.avg("pm25").alias("pm25"))
        .select("station_id", F.col("window.end").alias("timestamp"), "pm25")
        .crossJoin(latest)
        .filter(F.col("timestamp") <= F.col("last_observed"))
        .drop("last_observed")
    )
    features = hourly
    # Ghép đúng mốc thời gian để tạo độ trễ 1/2/3 giờ, tránh lệch khi thiếu giờ.
    for hours in (1, 2, 3):
        previous = hourly.select(
            "station_id",
            (F.col("timestamp") + F.expr(f"INTERVAL {hours} HOURS")).alias("timestamp"),
            F.col("pm25").alias(f"pm25_lag_{hours}"),
        )
        features = features.join(previous, ["station_id", "timestamp"], "left")
    # Bổ sung đặc trưng thời gian và loại các dòng chưa đủ đặc trưng.
    return (
        features.withColumn("hour_of_day", F.hour("timestamp").cast("double"))
        .withColumn("day_of_week", F.dayofweek("timestamp").cast("double"))
        .dropna(subset=FEATURE_COLUMNS)
    )


# Nhãn là PM2.5 tại thời điểm tương lai, cách mốc đầu vào horizon_hours giờ.
def build_training_frame(features, horizon_hours=1):
    labels = features.select(
        "station_id",
        F.col("timestamp").alias("label_time"),
        (F.col("timestamp") - F.expr(f"INTERVAL {int(horizon_hours)} HOURS")).alias(
            "timestamp"
        ),
        F.col("pm25").alias("label"),
    )
    return features.join(labels, ["station_id", "timestamp"])


# Chia theo thời gian: phần đầu để huấn luyện, phần sau để đánh giá.
def temporal_split(data, fraction=0.8):
    times = data.select(F.col("timestamp").cast("long").alias("t")).distinct()
    cutoff = times.approxQuantile("t", [fraction], 0.0)[0]
    # Nhãn của tập train không được vượt sang thời gian đầu vào của tập đánh giá.
    train = data.filter(F.col("label_time").cast("long") <= cutoff)
    test = data.filter(F.col("timestamp").cast("long") > cutoff)
    return train, test


# Lấy bộ đặc trưng mới nhất của từng trạm để tạo dự báo.
def latest_features(features):
    w = Window.partitionBy("station_id").orderBy(F.col("timestamp").desc())
    return (
        features.withColumn("_rank", F.row_number().over(w))
        .filter("_rank = 1")
        .drop("_rank")
    )

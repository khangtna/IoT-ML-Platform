"""Temporal model selection, full-data refit, then +1/+2/+3h inference."""

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from common import connect, upsert
from extract import read_sensor_metrics
from features import (
    build_feature_frame,
    build_training_frame,
    temporal_split,
    latest_features,
)
from train_linear_regression import train as lr
from train_random_forest import train as rf
from evaluate import evaluate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", help="ISO time bounding available history")
    args = parser.parse_args()
    # Mốc chạy cũng là giới hạn lịch sử đầu vào; mặc định lấy thời gian UTC hiện tại.
    as_of = (
        datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        if args.as_of
        else datetime.now(timezone.utc)
    )
    # Tạo phiên Spark; thống nhất múi giờ UTC khi xử lý các mốc thời gian.
    spark = (
        SparkSession.builder.appName("pm25-forecast-training")
        .config("spark.sql.session.timeZone", "UTC")
        # Driver Airflow và executor Spark dùng chung group 0 để ghi volume mô hình.
        .config("spark.hadoop.fs.permissions.umask-mode", "002")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    # Đọc dữ liệu và giữ đặc trưng trong cache để tái sử dụng cho nhiều mô hình.
    features = build_feature_frame(
        read_sensor_metrics(spark, as_of), as_of=as_of
    ).cache()
    report = {"as_of": as_of.isoformat(), "horizons": {}}
    try:
        inference = latest_features(features).cache()
        # Huấn luyện và chọn mô hình riêng cho từng khoảng dự báo +1/+2/+3 giờ.
        for horizon in [int(h) for h in os.getenv("FORECAST_HORIZONS", "1,2,3").split(",")]:
            if horizon not in (1, 2, 3):
                raise ValueError("FORECAST_HORIZONS must contain only 1,2,3")
            data = build_training_frame(features, horizon).cache()
            try:
                # Dừng sớm khi chưa đủ dữ liệu để huấn luyện và đánh giá.
                if data.count() < 50:
                    raise ValueError(
                        f"Insufficient hourly history for +{horizon}h; load at least 7 days of data"
                    )
                # Tách tập đánh giá nằm sau tập huấn luyện theo thời gian.
                train, test = temporal_split(data)
                train, test = train.cache(), test.cache()
                try:
                    if train.count() < 30 or test.count() < 10:
                        raise ValueError(
                            "Temporal split needs >=30 train and >=10 test rows"
                        )
                    scores = {}
                    # So sánh Linear Regression và Random Forest trên cùng tập đánh giá.
                    for name, fit in [("linear_regression", lr), ("random_forest", rf)]:
                        model = fit(train)
                        scores[name] = evaluate(model.transform(test))
                    # Chọn mô hình có RMSE thấp nhất cho khoảng dự báo này.
                    best = min(scores, key=lambda name: scores[name]["rmse"])
                    # Sau khi đánh giá, huấn luyện lại mô hình thắng trên toàn bộ dữ liệu hợp lệ.
                    final_model = {
                        "linear_regression": lr,
                        "random_forest": rf,
                    }[best](data)
                    # Lưu mô hình theo khoảng dự báo và lần chạy vào volume dùng chung.
                    model_path = f"/opt/project/models/pm25_{horizon}h_{as_of.strftime('%Y%m%dT%H%M%S%f')}"
                    final_model.write().overwrite().save(model_path)
                    # Dự báo từ mốc dữ liệu mới nhất của từng trạm; chặn PM2.5 âm về 0.
                    predicted = (
                        final_model.transform(inference)
                        .select(
                            "station_id",
                            (F.col("timestamp") + F.expr(f"INTERVAL {horizon} HOURS"))
                            .alias("forecast_time"),
                            F.greatest(F.lit(0.0), F.col("prediction"))
                            .alias("predicted_pm25"),
                        )
                        .withColumn("created_at", F.lit(as_of))
                        .withColumn("horizon_hours", F.lit(horizon))
                        .withColumn("model_name", F.lit(best))
                    )
                    # Ghi dự báo; cập nhật nếu đã tồn tại bản ghi cùng khóa chính.
                    upsert(
                        predicted,
                        "pm25_forecast",
                        ["created_at", "forecast_time", "station_id", "model_name"],
                    )
                    # Lưu điểm đánh giá của cả hai mô hình và cờ mô hình được chọn.
                    with connect() as conn, conn.cursor() as cur:
                        for name, metrics in scores.items():
                            cur.execute(
                                """INSERT INTO model_evaluations
                                (run_at,horizon_hours,model_name,mae,rmse,r2,selected)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (run_at,horizon_hours,model_name) DO UPDATE SET
                                mae=EXCLUDED.mae,rmse=EXCLUDED.rmse,r2=EXCLUDED.r2,selected=EXCLUDED.selected""",
                                (
                                    as_of,
                                    horizon,
                                    name,
                                    metrics["mae"],
                                    metrics["rmse"],
                                    metrics["r2"],
                                    name == best,
                                ),
                            )
                    report["horizons"][str(horizon)] = {
                        "metrics": scores,
                        "selected": best,
                        "model_path": model_path,
                    }
                finally:
                    train.unpersist()
                    test.unpersist()
            finally:
                data.unpersist()
        # Xuất báo cáo JSON để tra cứu kết quả và đường dẫn mô hình.
        target = Path("/opt/project/experiments/forecasting/latest_metrics.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Tạo file mới trong cùng volume rồi thay thế nguyên tử: không chmod file
        # có thể thuộc user Spark khác với user Airflow đang chạy driver.
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=target.parent,
                prefix=".latest_metrics-", suffix=".json", delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(json.dumps(report, indent=2))
                temporary.flush()
                os.fchmod(temporary.fileno(), 0o664)
            temporary_path.replace(target)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        print(json.dumps(report), flush=True)
    finally:
        features.unpersist()
        # Đóng phiên Spark và giải phóng tài nguyên sau khi kết thúc hoặc gặp lỗi.
        spark.stop()


if __name__ == "__main__":
    main()

"""Daily UTC aggregation precedes retraining and three-horizon inference."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Driver chạy trong scheduler; executor trên Spark Worker kết nối về hostname này.
CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.cores.max": "2",  # Giới hạn tổng số core của ứng dụng trên cluster.
    "spark.sql.shuffle.partitions": "3",  # Số partition khi shuffle Spark SQL.
}

with DAG(
    dag_id="pm25_forecast_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,  # Không chạy bù toàn bộ các khoảng lịch cũ.
    max_active_runs=1,  # Mỗi thời điểm chỉ có một DAG run hoạt động.
    is_paused_upon_creation=True,  # Chỉ bật lịch khi đã sẵn sàng chạy nghiệp vụ.
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["smart-city", "pm25"],
) as dag:
    # Tổng hợp dữ liệu trong khoảng [start, end) của lần chạy do Airflow cung cấp.
    aggregate = SparkSubmitOperator(
        task_id="aggregate_daily",
        application="/opt/project/spark/batch/daily_aggregation.py",
        conn_id="spark_default",  # Lấy connection từ biến môi trường trong Compose.
        conf=CONF,
        deploy_mode="client",
        application_args=[
            "--start",
            "{{ data_interval_start.isoformat() }}",
            "--end",
            "{{ data_interval_end.isoformat() }}",
        ],
    )

    # Dùng cuối khoảng lịch làm mốc giới hạn dữ liệu cho huấn luyện và dự báo.
    forecast = SparkSubmitOperator(
        task_id="train_and_forecast",
        application="/opt/project/spark/ml/ml_pipeline.py",
        conn_id="spark_default",
        conf=CONF,
        deploy_mode="client",
        application_args=[
            "--as-of",
            "{{ data_interval_end.isoformat() }}",
        ],
    )

    # Chỉ huấn luyện khi task tổng hợp đã thành công.
    aggregate >> forecast

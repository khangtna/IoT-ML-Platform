"""Daily UTC aggregation precedes retraining and three-horizon inference."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

CONF = {"spark.driver.host": "airflow-scheduler", "spark.driver.bindAddress": "0.0.0.0",
        "spark.cores.max": "2", "spark.sql.shuffle.partitions": "3"}

with DAG(dag_id="pm25_forecast_pipeline", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False, max_active_runs=1, is_paused_upon_creation=True,
         default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
         tags=["smart-city", "pm25"]) as dag:
    aggregate = SparkSubmitOperator(
        task_id="aggregate_daily", application="/opt/project/spark/batch/daily_aggregation.py",
        conn_id="spark_default", conf=CONF, deploy_mode="client",
        application_args=["--start", "{{ data_interval_start.isoformat() }}", "--end", "{{ data_interval_end.isoformat() }}"])
    forecast = SparkSubmitOperator(
        task_id="train_and_forecast", application="/opt/project/spark/ml/ml_pipeline.py",
        conn_id="spark_default", conf=CONF, deploy_mode="client",
        application_args=["--as-of", "{{ data_interval_end.isoformat() }}"])
    aggregate >> forecast

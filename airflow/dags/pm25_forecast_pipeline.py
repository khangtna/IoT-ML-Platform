from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
with DAG(dag_id="pm25_forecast_pipeline",start_date=datetime(2026,1,1),schedule="@daily",catchup=False,tags=["smart-city","pm25","spark"]) as dag:
 SparkSubmitOperator(task_id="train_and_forecast",application="/opt/airflow/project/spark/ml/ml_pipeline.py",conn_id="spark_default",packages="org.postgresql:postgresql:42.7.4",env_vars={"JDBC_URL":"jdbc:postgresql://timescaledb:5432/smartcity","POSTGRES_USER":"smartcity","POSTGRES_PASSWORD":"smartcity"})

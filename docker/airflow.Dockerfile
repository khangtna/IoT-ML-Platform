FROM apache/airflow:2.10.2-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.11.3 pyspark==3.5.3

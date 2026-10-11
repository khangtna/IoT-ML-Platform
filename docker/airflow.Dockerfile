FROM spark:3.5.3-scala2.12-java17-python3-ubuntu AS spark-runtime
FROM apache/airflow:2.10.2-python3.10
COPY --from=spark-runtime /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.10.2 apache-airflow-providers-apache-spark==4.11.3 pyspark==3.5.3 psycopg2-binary==2.9.10 numpy==1.26.4
COPY docker/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py
ENV PYTHONPATH=/opt/project/spark:/opt/project/spark/ml:/opt/project/spark/streaming
ENV PYSPARK_PYTHON=python3

USER root
RUN mkdir -p /opt/project/models /opt/project/experiments/forecasting && chown -R airflow:root /opt/project && chmod -R g+w /opt/project
USER airflow

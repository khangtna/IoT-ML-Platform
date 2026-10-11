FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10 numpy==1.26.4 && \
    mkdir -p /opt/checkpoints /opt/project/models /opt/project/experiments/forecasting && \
    chown -R spark:root /opt/checkpoints /opt/project && chmod -R g+w /opt/checkpoints /opt/project
COPY docker/fetch_jars.py /tmp/fetch_jars.py
RUN python3 /tmp/fetch_jars.py /opt/spark/jars
ENV PYTHONPATH=/opt/project/spark:/opt/project/spark/ml:/opt/project/spark/streaming
ENV PYSPARK_PYTHON=python3
USER spark:root

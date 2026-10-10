FROM spark:3.5.3-python3

USER root
RUN mkdir -p /opt/checkpoints && chown spark:spark /opt/checkpoints
USER spark

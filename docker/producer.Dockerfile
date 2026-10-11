FROM python:3.10-slim
WORKDIR /opt/project
COPY producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY producer /opt/project/producer
ENTRYPOINT ["python", "/opt/project/producer/sensor_simulator.py"]

FROM python:3.12-slim
WORKDIR /opt/project
COPY producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY producer /opt/project/producer
CMD ["python","/opt/project/producer/sensor_simulator.py","--data-dir","/opt/project/data/raw/beijing-multisite","--events-per-second","500"]

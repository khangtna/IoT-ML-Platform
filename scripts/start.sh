#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
if [[ ! -f .env ]]; then cp .env.example .env; fi
# Khởi động nền tảng Member 2; Kafka và các service nghiệp vụ đang được comment.
# Khi tích hợp Member 1: bật lại Kafka trong Compose và thêm kafka kafka-init vào lệnh dưới.
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 240 \
  timescaledb database-migrate spark-master spark-worker \
  grafana airflow-db airflow-init airflow-webserver airflow-scheduler

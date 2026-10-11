#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
if [[ ! -f .env ]]; then cp .env.example .env; fi
# Phase 1: start only foundation services; business services are commented in Compose.
"${COMPOSE[@]}" up -d --build --wait --wait-timeout 240 \
  kafka kafka-init timescaledb database-migrate spark-master spark-worker \
  grafana airflow-db airflow-init airflow-webserver airflow-scheduler

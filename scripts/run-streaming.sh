#!/usr/bin/env bash
set -euo pipefail
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4 /opt/project/spark/streaming/streaming_job.py

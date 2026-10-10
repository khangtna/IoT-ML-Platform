#!/usr/bin/env bash
set -euo pipefail
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.4 /opt/project/spark/ml/ml_pipeline.py

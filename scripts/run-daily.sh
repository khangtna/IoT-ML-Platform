#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
"${COMPOSE[@]}" exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --conf spark.driver.host=spark-master \
  --conf spark.cores.max=2 --conf spark.sql.shuffle.partitions=3 \
  /opt/project/spark/batch/daily_aggregation.py "$@"

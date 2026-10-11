#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
mkdir -p experiments/forecasting
"${COMPOSE[@]}" cp spark-master:/opt/project/experiments/forecasting/latest_metrics.json experiments/forecasting/latest_metrics.json

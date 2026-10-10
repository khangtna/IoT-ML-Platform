# Smart City IoT Platform for Environmental Forecasting

PoC môn cao học Big Data.

## Scope
- Environmental Monitoring
- Dataset: Beijing Multi-Site Air-Quality Data
- Target: PM2.5
- Kafka >= 500 events/s
- Spark Structured Streaming
- Rolling Z-score anomaly detection
- Spark MLlib forecasting
- Apache Airflow orchestration
- TimescaleDB storage
- Grafana visualization
- Docker Compose runtime

## Architecture
```text
Dataset -> Producer -> Kafka -> Spark Streaming -> TimescaleDB.sensor_metrics -> Grafana
                                              \-> Airflow -> Spark MLlib -> TimescaleDB.pm25_forecast -> Grafana
```

## Canonical event schema
```json
{
  "event_id": "Aotizhongxin-2016-05-01T10:00:00+08:00",
  "station_id": "Aotizhongxin",
  "timestamp": "2016-05-01T10:00:00+08:00",
  "pm25": 45.2,
  "pm10": 68.1,
  "so2": 8.0,
  "no2": 42.0,
  "co": 620.0,
  "o3": 55.0,
  "temperature": 31.4,
  "pressure": 1008.3,
  "dew_point": 24.2,
  "rainfall": 0.0,
  "wind_direction": "NE",
  "wind_speed": 2.4
}
```
Required: `event_id`, `station_id`, `timestamp`, `pm25`. Missing values: `NA -> null`, never `0`.

## Processed schema
`timestamp, station_id, pm25, pm10, so2, no2, co, o3, temperature, pressure, dew_point, rainfall, wind_direction, wind_speed, pm25_avg_5m, pm25_rolling_mean_30m, pm25_rolling_std_30m, pm25_z_score, is_anomaly`

## ML contract
Input: `sensor_metrics`. Initial target: `pm25(t+1h)`; planned +2h/+3h. Models: Linear Regression and Random Forest. Metrics: MAE, RMSE, R².

## Team ownership
- Member 1: `producer/`, `spark/streaming/`
- Member 2: `spark/ml/`, `airflow/`, `grafana/`
- Shared: `database/`, `docker-compose.yml`, `README.md`, `docs/`

## Quick start
1. Put Beijing CSV files under `data/raw/beijing-multisite/`.
2. `cp .env.example .env`
3. `docker compose up -d --build`
4. `./scripts/run-producer.sh`
5. `./scripts/run-streaming.sh`
6. Open Grafana `http://localhost:3000` (admin/admin), Airflow `http://localhost:8088` (admin/admin), Spark master `http://localhost:8080`.
7. Trigger Airflow DAG `pm25_forecast_pipeline` or run `./scripts/run-ml.sh`.

## PoC limitations
Spark uses the official `spark:3.5.3-python3` image, matching PySpark and the Kafka connector. The local Dockerfile prepares a writable checkpoint directory. After updating, run `docker compose up -d --build` to rebuild the Spark services.

- Rolling 30-minute Z-score is implemented as a micro-batch approximation. Exact cross-batch event-time rolling state should use stateful processing.
- Forecast generation uses the held-out evaluation frame as a simple lab demonstration; production inference should score the latest actual measurements.

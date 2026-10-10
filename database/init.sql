CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_metrics (
 timestamp TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL,
 pm25 DOUBLE PRECISION, pm10 DOUBLE PRECISION, so2 DOUBLE PRECISION, no2 DOUBLE PRECISION, co DOUBLE PRECISION, o3 DOUBLE PRECISION,
 temperature DOUBLE PRECISION, pressure DOUBLE PRECISION, dew_point DOUBLE PRECISION, rainfall DOUBLE PRECISION, wind_direction TEXT, wind_speed DOUBLE PRECISION,
 pm25_avg_5m DOUBLE PRECISION, pm25_rolling_mean_30m DOUBLE PRECISION, pm25_rolling_std_30m DOUBLE PRECISION, pm25_z_score DOUBLE PRECISION, is_anomaly BOOLEAN,
 PRIMARY KEY(timestamp,station_id));
SELECT create_hypertable('sensor_metrics','timestamp',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS idx_sensor_metrics_station_time ON sensor_metrics(station_id,timestamp DESC);

CREATE TABLE IF NOT EXISTS pm25_forecast (
 created_at TIMESTAMPTZ NOT NULL, forecast_time TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, horizon_hours INTEGER NOT NULL CHECK(horizon_hours>0), predicted_pm25 DOUBLE PRECISION NOT NULL, model_name TEXT NOT NULL,
 PRIMARY KEY(created_at,forecast_time,station_id,model_name));
SELECT create_hypertable('pm25_forecast','forecast_time',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS idx_pm25_forecast_station_time ON pm25_forecast(station_id,forecast_time DESC);

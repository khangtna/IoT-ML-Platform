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

-- Version 2 additions are additive and safe on an existing volume.
ALTER TABLE sensor_metrics ADD COLUMN IF NOT EXISTS pm25_imputed BOOLEAN DEFAULT false;
ALTER TABLE sensor_metrics ADD COLUMN IF NOT EXISTS is_threshold_exceeded BOOLEAN DEFAULT false;
CREATE TABLE IF NOT EXISTS sensor_raw (
    timestamp TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, event_id TEXT,
    pm25 DOUBLE PRECISION, pm10 DOUBLE PRECISION, so2 DOUBLE PRECISION,
    no2 DOUBLE PRECISION, co DOUBLE PRECISION, o3 DOUBLE PRECISION,
    temperature DOUBLE PRECISION, pressure DOUBLE PRECISION, dew_point DOUBLE PRECISION,
    rainfall DOUBLE PRECISION, wind_direction TEXT, wind_speed DOUBLE PRECISION,
    PRIMARY KEY(timestamp, station_id)
);
SELECT create_hypertable('sensor_raw','timestamp',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS idx_sensor_raw_station_time ON sensor_raw(station_id,timestamp DESC);
CREATE TABLE IF NOT EXISTS sensor_windows (
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL, window_type TEXT NOT NULL,
    pm25_avg DOUBLE PRECISION, sample_count BIGINT NOT NULL,
    PRIMARY KEY(window_start, station_id, window_type)
);
SELECT create_hypertable('sensor_windows','window_start',if_not_exists=>TRUE);
CREATE TABLE IF NOT EXISTS daily_summary (
    day DATE NOT NULL, station_id TEXT NOT NULL, event_count BIGINT NOT NULL,
    valid_pm25_count BIGINT NOT NULL, pm25_avg DOUBLE PRECISION, pm25_max DOUBLE PRECISION,
    anomaly_count BIGINT, threshold_count BIGINT,
    PRIMARY KEY(day,station_id)
);
CREATE TABLE IF NOT EXISTS model_evaluations (
    run_at TIMESTAMPTZ NOT NULL, horizon_hours INTEGER NOT NULL,
    model_name TEXT NOT NULL, mae DOUBLE PRECISION, rmse DOUBLE PRECISION,
    r2 DOUBLE PRECISION, selected BOOLEAN NOT NULL,
    PRIMARY KEY(run_at,horizon_hours,model_name)
);
CREATE OR REPLACE VIEW active_alerts AS
SELECT timestamp,station_id,pm25,pm25_z_score,is_anomaly,is_threshold_exceeded
FROM sensor_metrics WHERE is_anomaly OR is_threshold_exceeded;
CREATE OR REPLACE VIEW latest_forecasts AS
SELECT DISTINCT ON (station_id,forecast_time,horizon_hours)
    created_at,forecast_time,station_id,horizon_hours,predicted_pm25,model_name
FROM pm25_forecast
ORDER BY station_id,forecast_time,horizon_hours,created_at DESC;

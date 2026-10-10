CREATE TABLE IF NOT EXISTS pm25_forecast (
 created_at TIMESTAMPTZ NOT NULL, forecast_time TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, horizon_hours INTEGER NOT NULL CHECK(horizon_hours>0), predicted_pm25 DOUBLE PRECISION NOT NULL, model_name TEXT NOT NULL,
 PRIMARY KEY(created_at,forecast_time,station_id,model_name));
SELECT create_hypertable('pm25_forecast','forecast_time',if_not_exists=>TRUE);
CREATE INDEX IF NOT EXISTS idx_pm25_forecast_station_time ON pm25_forecast(station_id,forecast_time DESC);

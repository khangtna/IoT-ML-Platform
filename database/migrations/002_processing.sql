-- Bổ sung schema xử lý dữ liệu; cần có bảng sensor_metrics và pm25_forecast trước.

-- Bổ sung cột, bảng và view; không xóa dữ liệu hiện có.
-- Đánh dấu PM2.5 được điền bù/nội suy; tác vụ xử lý dữ liệu cập nhật cờ này.
ALTER TABLE sensor_metrics ADD COLUMN IF NOT EXISTS pm25_imputed BOOLEAN DEFAULT false;
-- Đánh dấu vượt ngưỡng PM2.5, tách biệt với bất thường theo Z-score.
ALTER TABLE sensor_metrics ADD COLUMN IF NOT EXISTS is_threshold_exceeded BOOLEAN DEFAULT false;
-- Dữ liệu cảm biến đầu vào, chưa có các thống kê và cờ xử lý.
-- Khóa chính giới hạn một bản ghi cho mỗi trạm tại mỗi thời điểm.
CREATE TABLE IF NOT EXISTS sensor_raw (
    timestamp TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, event_id TEXT,
    pm25 DOUBLE PRECISION, pm10 DOUBLE PRECISION, so2 DOUBLE PRECISION,
    no2 DOUBLE PRECISION, co DOUBLE PRECISION, o3 DOUBLE PRECISION,
    temperature DOUBLE PRECISION, pressure DOUBLE PRECISION, dew_point DOUBLE PRECISION,
    rainfall DOUBLE PRECISION, wind_direction TEXT, wind_speed DOUBLE PRECISION,
    PRIMARY KEY(timestamp, station_id)
);
-- Chuyển thành hypertable, chia dữ liệu theo thời gian ở cột timestamp.
SELECT create_hypertable('sensor_raw','timestamp',if_not_exists=>TRUE);
-- Tăng tốc truy vấn dữ liệu theo trạm và thời gian mới nhất.
CREATE INDEX IF NOT EXISTS idx_sensor_raw_station_time ON sensor_raw(station_id,timestamp DESC);
-- Tổng hợp theo cửa sổ: thời gian bắt đầu/kết thúc, loại cửa sổ, trung bình và số mẫu.
CREATE TABLE IF NOT EXISTS sensor_windows (
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL, window_type TEXT NOT NULL,
    pm25_avg DOUBLE PRECISION, sample_count BIGINT NOT NULL,
    PRIMARY KEY(window_start, station_id, window_type)
);
-- Chuyển thành hypertable, chia dữ liệu theo thời gian ở cột window_start.
SELECT create_hypertable('sensor_windows','window_start',if_not_exists=>TRUE);
-- Tổng hợp theo ngày và trạm; tác vụ do Airflow điều phối sẽ tính và ghi dữ liệu.
-- Lưu số bản ghi, số PM2.5 hợp lệ, trung bình/max và số lần cảnh báo.
CREATE TABLE IF NOT EXISTS daily_summary (
    day DATE NOT NULL, station_id TEXT NOT NULL, event_count BIGINT NOT NULL,
    valid_pm25_count BIGINT NOT NULL, pm25_avg DOUBLE PRECISION, pm25_max DOUBLE PRECISION,
    anomaly_count BIGINT, threshold_count BIGINT,
    PRIMARY KEY(day,station_id)
);
-- Đánh giá từng mô hình và khoảng dự báo theo lần chạy: MAE, RMSE, R².
-- selected đánh dấu mô hình được pipeline chọn; bảng này không tự chọn mô hình.
CREATE TABLE IF NOT EXISTS model_evaluations (
    run_at TIMESTAMPTZ NOT NULL, horizon_hours INTEGER NOT NULL,
    model_name TEXT NOT NULL, mae DOUBLE PRECISION, rmse DOUBLE PRECISION,
    r2 DOUBLE PRECISION, selected BOOLEAN NOT NULL,
    PRIMARY KEY(run_at,horizon_hours,model_name)
);
-- Lọc bản ghi bất thường hoặc vượt ngưỡng; Grafana cần thêm bộ lọc thời gian.
CREATE OR REPLACE VIEW active_alerts AS
SELECT timestamp,station_id,pm25,pm25_z_score,is_anomaly,is_threshold_exceeded
FROM sensor_metrics WHERE is_anomaly OR is_threshold_exceeded;
-- Lấy dự báo có created_at mới nhất cho mỗi trạm, thời điểm và khoảng dự báo.
-- Không lọc theo selected; nếu nhiều mô hình cùng created_at thì chưa có ưu tiên rõ ràng.
CREATE OR REPLACE VIEW latest_forecasts AS
SELECT DISTINCT ON (station_id,forecast_time,horizon_hours)
    created_at,forecast_time,station_id,horizon_hours,predicted_pm25,model_name
FROM pm25_forecast
ORDER BY station_id,forecast_time,horizon_hours,created_at DESC;

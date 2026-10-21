-- Schema riêng cho bảng; cần bật extension timescaledb trước khi chạy.

-- Dữ liệu cảm biến đã xử lý: chỉ số môi trường, thống kê cửa sổ và cờ bất thường.
-- Khóa (timestamp, station_id) giữ một bản ghi cho mỗi trạm tại mỗi thời điểm.
CREATE TABLE IF NOT EXISTS sensor_metrics (
 timestamp TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL,
 -- Các chỉ số ô nhiễm đo được tại trạm.
 pm25 DOUBLE PRECISION, pm10 DOUBLE PRECISION, so2 DOUBLE PRECISION, no2 DOUBLE PRECISION, co DOUBLE PRECISION, o3 DOUBLE PRECISION,
 -- Các chỉ số thời tiết đi kèm.
 temperature DOUBLE PRECISION, pressure DOUBLE PRECISION, dew_point DOUBLE PRECISION, rainfall DOUBLE PRECISION, wind_direction TEXT, wind_speed DOUBLE PRECISION,
 -- Trung bình 5 phút; trung bình/độ lệch chuẩn trượt 30 phút; Z-score và cờ bất thường.
 pm25_avg_5m DOUBLE PRECISION, pm25_rolling_mean_30m DOUBLE PRECISION, pm25_rolling_std_30m DOUBLE PRECISION, pm25_z_score DOUBLE PRECISION, is_anomaly BOOLEAN,
 PRIMARY KEY(timestamp,station_id));
-- Chuyển thành hypertable, chia dữ liệu theo thời gian ở cột timestamp.
SELECT create_hypertable('sensor_metrics','timestamp',if_not_exists=>TRUE);
-- Tăng tốc truy vấn dữ liệu theo trạm và thời gian mới nhất.
CREATE INDEX IF NOT EXISTS idx_sensor_metrics_station_time ON sensor_metrics(station_id,timestamp DESC);

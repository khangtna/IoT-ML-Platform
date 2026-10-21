-- Schema riêng cho bảng; cần bật extension timescaledb trước khi chạy.

-- Kết quả dự báo từ Spark MLlib, dùng cho dashboard Grafana.
-- created_at: thời điểm chạy; forecast_time: thời điểm tương lai được dự báo.
-- horizon_hours: số giờ dự báo trước (đề bài dùng 1–3; CHECK hiện chỉ yêu cầu > 0).
-- predicted_pm25: giá trị dự báo; model_name: mô hình tạo kết quả.
-- Khóa chính giữ lịch sử theo lần chạy, thời điểm dự báo, trạm và mô hình.
CREATE TABLE IF NOT EXISTS pm25_forecast (
 created_at TIMESTAMPTZ NOT NULL, forecast_time TIMESTAMPTZ NOT NULL, station_id TEXT NOT NULL, horizon_hours INTEGER NOT NULL CHECK(horizon_hours>0), predicted_pm25 DOUBLE PRECISION NOT NULL, model_name TEXT NOT NULL,
 PRIMARY KEY(created_at,forecast_time,station_id,model_name));
-- Chuyển thành hypertable, chia dữ liệu theo thời gian ở cột forecast_time.
SELECT create_hypertable('pm25_forecast','forecast_time',if_not_exists=>TRUE);
-- Tăng tốc truy vấn dữ liệu theo trạm và thời gian mới nhất.
CREATE INDEX IF NOT EXISTS idx_pm25_forecast_station_time ON pm25_forecast(station_id,forecast_time DESC);

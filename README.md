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

![Kiến trúc tổng quan Smart City IoT Big Data](docs/smart_city_bigdata_architecture.png)

[Chỉnh sửa sơ đồ Draw.io](docs/smart_city_bigdata_architecture.drawio) · [Xem bản SVG](docs/smart_city_bigdata_architecture.svg)

Sơ đồ được dựng từ file `smart_city_bigdata_architecture.drawio` trong thư mục cha `tieu-luan/`. Hệ thống có hai luồng xử lý:

- **Streaming:** Producer đọc CSV và mô phỏng sự kiện cảm biến gửi vào Kafka. Spark Structured Streaming chuẩn hóa dữ liệu, tính trung bình PM2.5 theo cửa sổ 5 phút và phát hiện bất thường bằng Rolling Z-score, sau đó ghi vào `sensor_metrics`. Grafana truy vấn TimescaleDB để hiển thị số liệu và bất thường.
- **Batch/ML:** Airflow lập lịch DAG chạy Spark MLlib. Job đọc lịch sử từ `sensor_metrics`, tạo đặc trưng, huấn luyện Linear Regression và Random Forest, đánh giá bằng MAE/RMSE/R², rồi ghi dự báo của mô hình có RMSE thấp nhất vào `pm25_forecast` để Grafana hiển thị.

```text
Dataset -> Producer -> Kafka -> Spark Streaming -> TimescaleDB.sensor_metrics -> Grafana
Airflow -> Spark MLlib (đọc TimescaleDB.sensor_metrics) -> TimescaleDB.pm25_forecast -> Grafana
```

Sơ đồ thể hiện kiến trúc tổng quan và hướng mở rộng. PoC hiện dùng TimescaleDB, dữ liệu Beijing và dự báo PM2.5 +1h; các mốc +2h/+3h và cơ sở dữ liệu thay thế trong hình là định hướng mở rộng. Anomaly Detection được triển khai ngay trong job streaming.

## Cấu trúc thư mục

Các chú thích sau dấu `#` mô tả vai trò của thư mục hoặc file:

```text
smart-city-iot-bigdata-complete/
├── README.md                         # Tổng quan, kiến trúc và hướng dẫn chạy
├── .env.example                      # Mẫu biến môi trường cho Docker Compose
├── docker-compose.yml                # Khai báo dịch vụ, mạng, volume và thứ tự khởi động
├── producer/                         # Mô phỏng dữ liệu cảm biến và gửi vào Kafka
│   ├── sensor_simulator.py           # Đọc CSV Beijing, tạo JSON, điều tiết tốc độ gửi
│   └── requirements.txt              # Thư viện Python cần cho producer
├── spark/
│   ├── streaming/                    # Pipeline xử lý dữ liệu thời gian thực
│   │   ├── streaming_job.py          # Điều phối streaming, checkpoint và ghi JDBC
│   │   ├── kafka_consumer.py         # Đọc topic Kafka và giải mã sự kiện JSON
│   │   ├── schemas.py                # Khai báo schema sự kiện cảm biến
│   │   ├── transform.py              # Chuẩn hóa và làm sạch dữ liệu
│   │   └── anomaly_detection.py      # Tính Rolling Z-score và đánh dấu bất thường
│   └── ml/                           # Pipeline huấn luyện và dự báo PM2.5
│       ├── ml_pipeline.py            # Điều phối huấn luyện, chọn mô hình và ghi dự báo
│       ├── extract.py                # Đọc dữ liệu sensor_metrics qua JDBC
│       ├── features.py               # Tạo đặc trưng và nhãn dự báo
│       ├── train_linear_regression.py # Huấn luyện Linear Regression
│       ├── train_random_forest.py    # Huấn luyện Random Forest Regressor
│       └── evaluate.py               # Đánh giá MAE, RMSE và R²
├── airflow/
│   ├── dags/pm25_forecast_pipeline.py # DAG lập lịch chạy job Spark MLlib
│   └── logs/                         # Log thực thi Airflow, được mount vào container
├── database/                         # SQL tạo cấu trúc lưu trữ TimescaleDB
│   ├── init.sql                      # Script khởi tạo khi database volume còn trống
│   ├── schema_sensor_metrics.sql     # Schema dữ liệu đo và kết quả phát hiện bất thường
│   └── schema_pm25_forecast.sql       # Schema kết quả dự báo PM2.5
├── grafana/
│   ├── dashboards/                   # Dashboard thời gian thực, bất thường và dự báo
│   └── provisioning/                 # Tự động cấu hình datasource và nạp dashboard
├── docker/
│   ├── producer.Dockerfile           # Image chạy producer Python
│   ├── spark.Dockerfile              # Image Spark 3.5.3 và quyền ghi checkpoint
│   └── airflow.Dockerfile            # Image Airflow có Java, PySpark và Spark provider
├── scripts/                          # Lệnh hỗ trợ chạy từ thư mục gốc dự án
│   ├── start.sh                      # Chuẩn bị .env và khởi động các dịch vụ
│   ├── stop.sh                       # Dừng các dịch vụ
│   ├── run-producer.sh               # Chạy producer với dữ liệu CSV
│   ├── run-streaming.sh              # Submit job streaming vào Spark cluster
│   └── run-ml.sh                     # Submit job ML vào Spark cluster
├── data/
│   ├── raw/beijing-multisite/        # Nơi đặt CSV Beijing trước khi chạy producer
│   ├── sample/sample_events.json     # Ví dụ sự kiện cảm biến theo schema chuẩn
│   └── processed/                    # Thư mục dự phòng cho dữ liệu đã xử lý
├── models/                           # Thư mục dự phòng để lưu mô hình
├── experiments/                      # Nơi lưu kết quả thí nghiệm của nhóm
│   ├── throughput/                   # Thí nghiệm tốc độ xử lý
│   ├── anomaly/                      # Thí nghiệm phát hiện bất thường
│   └── forecasting/                  # Thí nghiệm dự báo và so sánh mô hình
└── docs/                             # Tài liệu, sơ đồ kiến trúc và hình minh họa
    ├── smart_city_bigdata_architecture.drawio # Bản nguồn sơ đồ dùng trong README
    ├── smart_city_bigdata_architecture.png    # Hình kiến trúc hiển thị trong README
    ├── smart_city_bigdata_architecture.svg    # Bản vector để phóng to
    ├── architecture.drawio           # Sơ đồ có sẵn của dự án
    ├── report/                       # Báo cáo tiểu luận
    └── slides/                       # Slide thuyết trình
```

Tạo `data/raw/beijing-multisite/` nếu chưa có và đặt các file CSV vào đó. Các thư mục `data/processed/`, `models/`, `experiments/`, `docs/report/` và `docs/slides/` hiện là nơi dự phòng; pipeline chưa tự động ghi kết quả vào các thư mục này. Dữ liệu database, Grafana và checkpoint Spark được lưu trong các named volume khai báo ở `docker-compose.yml`.

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

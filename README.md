# Smart City IoT Platform for Environmental Forecasting

Đề tài 4 môn cao học Big Data: hệ thống giám sát môi trường và dự báo PM2.5 từ cảm biến IoT. Triển khai nhánh **môi trường** của đề tài, dùng Kafka, Spark, Airflow, TimescaleDB và Grafana.

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

Hình Draw.io là sơ đồ định hướng ban đầu. Bản triển khai hiện dùng TimescaleDB, có dự báo **+1/+2/+3 giờ** và job tổng hợp cuối ngày. Anomaly Detection nằm trong streaming job. Chi tiết trạng thái qua nhiều micro-batch, các bảng dữ liệu và luồng triển khai nằm trong [kiến trúc triển khai](docs/architecture.md).

## Đối chiếu yêu cầu đề tài

| Yêu cầu trong `de-tai.md` | Phần triển khai |
| --- | --- |
| Kafka nhận ≥500 events/s | Producer điều tiết tốc độ, 3 partitions, đo số bản tin được broker xác nhận |
| Làm sạch và nội suy | Loại timestamp/trạm không hợp lệ; đổi số âm/NaN/∞ thành null; nội suy tuyến tính có hai đầu mút, khoảng cách tối đa 3 giờ |
| Tumbling/sliding windows | Tumbling 5 phút, sliding 30 phút bước 5 phút trong `sensor_windows` |
| Rolling Z-score | Baseline 30 phút trước từng điểm, tách theo trạm, dùng lịch sử đã lưu qua nhiều batch |
| Cảnh báo | Z-score và ngưỡng PM2.5 cấu hình; dashboard + Grafana-managed alert |
| Tổng hợp cuối ngày | Spark ghi `daily_summary`, Airflow chạy trước job ML |
| Hồi quy phân tán 1–3 giờ | Linear Regression/Random Forest trên Spark, ba horizon, holdout theo thời gian |
| Time-series database | TimescaleDB hypertables; upsert theo khóa nghiệp vụ để chạy lại an toàn |
| Grafana và heatmap | 4 dashboard, lọc theo trạm, heatmap phân bố nồng độ theo thời gian |

500 events/s là mục tiêu đầu vào cần đo trên máy chạy; chưa đồng nghĩa throughput xử lý Spark đạt 500 events/s. Xem [quy trình đo và nghiệm thu](docs/report/validation.md).

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
│   ├── common.py                     # JDBC và PostgreSQL upsert dùng chung
│   ├── batch/daily_aggregation.py     # Tổng hợp số liệu theo ngày UTC
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
│   └── logs/                         # Thư mục có sẵn; runtime dùng named volume airflow-logs
├── database/                         # SQL tạo cấu trúc lưu trữ TimescaleDB
│   ├── init.sql                      # Schema đầy đủ cho database mới
│   ├── migrations/                   # Migration bổ sung cho database đã tồn tại
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
│   ├── run-ml.sh                     # Submit job ML vào Spark cluster
├── data/
│   ├── raw/beijing-multisite/        # Nơi đặt CSV Beijing trước khi chạy producer
│   ├── sample/sample_events.json     # Ví dụ sự kiện cảm biến theo schema chuẩn
│   └── processed/                    # Thư mục dự phòng cho dữ liệu đã xử lý
├── models/                           # Điểm mount trong container cho volume chứa mô hình
├── tests/test_pipeline.py            # Test thời gian, nội suy, Z-score và chạy lại
├── experiments/                      # Nơi lưu kết quả thí nghiệm của nhóm
│   ├── throughput/                   # Thí nghiệm tốc độ xử lý
│   ├── anomaly/                      # Thí nghiệm phát hiện bất thường
│   └── forecasting/                  # Thí nghiệm dự báo và so sánh mô hình
└── docs/                             # Tài liệu, sơ đồ kiến trúc và hình minh họa
```

Tạo `data/raw/beijing-multisite/` và đặt CSV vào đó nếu dùng dữ liệu thật. Producer ghi kết quả tốc độ vào `experiments/throughput/latest.json`. Mô hình, metrics ML, log Airflow, Kafka, database và checkpoint được lưu trong named volumes; dùng `scripts/export-results.sh` để chép metrics ML ra máy host. `data/processed/` vẫn là thư mục dự phòng.

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
Khóa nghiệp vụ: `(timestamp, station_id)`. `event_id` dùng truy vết; bản tin phải có trạm và timestamp hợp lệ. PM2.5 thiếu được giữ dưới dạng null để nội suy; `NA` không được thay bằng 0. Các trường ô nhiễm dùng đơn vị của nguồn Beijing, PM2.5 là µg/m³. Dataset có **CO**, không phải CO2; chưa có trường độ ẩm tương đối.

## Processed schema
`sensor_metrics` lưu các phép đo cùng `pm25_imputed`, `pm25_avg_5m`, `pm25_rolling_mean_30m`, `pm25_rolling_std_30m`, `pm25_z_score`, `is_anomaly`, `is_threshold_exceeded`. Schema đầy đủ và các bảng `sensor_raw`, `sensor_windows`, `daily_summary`, `pm25_forecast`, `model_evaluations` nằm trong `database/init.sql`.

## ML contract
Dữ liệu được tổng hợp về giờ đã hoàn tất; timestamp của feature là **cuối giờ**, tức thời điểm dữ liệu đã sẵn sàng. Nhãn tại `t+h` là trung bình PM2.5 của giờ kết thúc tại `t+h`, với `h ∈ {1,2,3}`. Lag/label được nối bằng timestamp chính xác theo trạm, không dùng số thứ tự dòng để suy ra số giờ.

Feature gồm PM2.5, lag 1/2/3 giờ, giờ trong ngày và ngày trong tuần. Bỏ các giá trị PM2.5 nội suy hai chiều khỏi dữ liệu ML để tránh dùng thông tin tương lai. Chia khoảng 80/20 theo thời gian; nhãn của tập train phải kết thúc trước holdout. Chọn LR/RF theo RMSE, ghi MAE/RMSE/R², refit trên toàn bộ lịch sử đủ nhãn rồi dự báo từ feature mới nhất của từng trạm. Mô hình lưu theo thời điểm chạy trong volume `spark-models`.

## Team ownership
- Member 1: `producer/`, `spark/streaming/`
- Member 2: `spark/ml/`, `airflow/`, `grafana/`
- Shared: `database/`, `docker-compose.yml`, `README.md`, `docs/`

## Quick start

Cần Docker Desktop đang chạy; cấu hình hiện tại nên dành khoảng 8 GB RAM cho Docker. Build lần đầu cần truy cập Docker Hub, PyPI và Maven Central. Python 3.10 được đồng bộ giữa Spark và Airflow; JDBC/Kafka JAR được đóng gói khi build để runtime không phải tải lại.

```bash
# Chạy từ thư mục dự án
./scripts/start.sh
# Tạo dữ liệu demo: 7 ngày, 4 trạm, khoảng cách 1 phút, gửi mục tiêu 500 events/s
./scripts/run-producer.sh --synthetic --hours 168 --stations 4 --events-per-second 500
```

Streaming tự chạy như một service. Chờ backlog Kafka được xử lý xong trước khi chạy ML:

```bash
# Các script tự chọn docker compose hoặc docker-compose;
# hai lệnh bên dưới dùng docker-compose (đổi thành docker compose nếu cần)
docker-compose logs --tail 50 streaming
docker-compose exec -T timescaledb sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*), min(timestamp), max(timestamp) FROM sensor_metrics"'
./scripts/run-ml.sh
./scripts/run-daily.sh
./scripts/export-results.sh
```

Tổng số dữ liệu demo dự kiến: `168 × 60 × 4 = 40.320` khóa trạm/thời gian. Dữ liệu demo có null và spike cố định để kiểm tra nội suy/cảnh báo. Đây là dữ liệu mô phỏng, không dùng metrics của nó làm kết luận về độ chính xác trên dữ liệu thực.

| Giao diện | Địa chỉ | Đăng nhập mặc định |
| --- | --- | --- |
| Grafana | http://localhost:3000 | admin / admin |
| Airflow | http://localhost:8088 | admin / admin |
| Spark master | http://localhost:8080 | Không yêu cầu |
| Spark worker | http://localhost:8081 | Không yêu cầu |

Trong Grafana mở folder **Smart City**: giám sát, cảnh báo, dự báo, heatmap. Chọn trạm và khoảng thời gian phù hợp. Dự báo +1/+2/+3 giờ được tính từ **giờ quan sát cuối cùng**, không từ đồng hồ hiện tại nếu đang replay lịch sử. Alert tự động xét 10 phút event-time gần hiện tại; lịch sử cũ vẫn hiển thị trên dashboard nhưng không kích hoạt alert hiện thời.

Airflow DAG `pm25_forecast_pipeline` được bật, chạy `@daily` theo UTC: `aggregate_daily → train_and_forecast`. Có thể trigger thủ công sau khi nạp đủ lịch sử; DAG dùng `data_interval_start/end` và retry một lần. Thiếu dữ liệu huấn luyện sẽ báo lỗi rõ ràng.

### Dữ liệu Beijing thực

Tải [Beijing Multi-Site Air Quality từ UCI](https://www.archive.ics.uci.edu/dataset/501/beijing%2Bmulti%2Bsite%2Bair%2Bquality%2Bdata), giải nén CSV vào `data/raw/beijing-multisite/`, rồi chạy:

```bash
./scripts/run-producer.sh --events-per-second 500
```

Producer merge các file theo timestamp để các trạm được replay cùng thời gian. CSV từng trạm phải được sắp tăng dần theo thời gian. Dataset có 420.768 bản ghi theo giờ tại 12 trạm (2013–2017); cần chọn khoảng lịch sử tương ứng trong Grafana. Tần suất gốc 1 giờ không đủ mẫu cho Z-score cửa sổ 30 phút; dùng demo 1 phút để minh họa thuật toán này, không coi tốc độ replay là tần suất đo.

### Database có sẵn và chạy lại

Service `database-migrate` áp dụng migration bổ sung, giữ dữ liệu hiện có. `init.sql` chỉ tự chạy khi tạo volume mới. Các file `schema_*.sql` là trích đoạn schema ban đầu; dùng `init.sql` và migration làm nguồn schema triển khai.

Checkpoint mới `/opt/checkpoints/smart-city-v2` đọc Kafka từ earliest ở lần đầu. Restart dùng offset trong checkpoint. Sink upsert theo khóa nghiệp vụ nên replay không tạo dòng trùng. Không chạy hai streaming driver dùng cùng checkpoint. Để dừng mà giữ dữ liệu:

```bash
./scripts/stop.sh
```

### Kiểm thử và kết quả thí nghiệm

```bash
./scripts/test.sh
# Benchmark đầu vào lớn hơn: 1.008.000 events, 100 trạm, 7 ngày
./scripts/run-producer.sh --synthetic --hours 168 --stations 100 --events-per-second 1000
```

Test tích hợp tạo database tạm `smartcity_test_<pid>` và xóa riêng database đó khi xong. Xem [kế hoạch nghiệm thu](docs/report/validation.md) để đo throughput, dữ liệu đến muộn, khôi phục và chất lượng dự báo. Các lệnh benchmark triệu bản tin có chủ đích và cần theo dõi bộ nhớ/backlog; chưa được coi là đạt nếu chưa có số đo.

## Giới hạn triển khai

- Đây là PoC một máy, Kafka replication factor 1, một Spark worker và một TimescaleDB; chưa có HA hay benchmark chứng minh quy mô triệu bản tin.
- Lịch sử được giữ ở TimescaleDB để recompute một khoảng event-time có giới hạn qua nhiều batch. Bản tin đến muộn có thể sửa kết quả nội suy và cửa sổ cũ. Mỗi partition ghi transaction riêng; dashboard có thể thấy dữ liệu cập nhật dần, không phải một transaction cho cả batch.
- Nội suy chỉ lấp giá trị null của bản tin đã có hai đầu mút đủ gần. Không tự sinh bản tin cho khoảng sensor hoàn toàn mất kết nối; không ngoại suy đầu/cuối chuỗi. Z-score cần ít nhất 3 điểm và độ lệch chuẩn >0; ngưỡng 75 µg/m³ chỉ là cấu hình demo.
- Heatmap thể hiện phân bố nồng độ theo thời gian. Dataset/schema hiện chưa có tọa độ để làm bản đồ địa lý.
- Tổng hợp ngày được cập nhật khi DAG chạy cho khoảng ngày đó; dữ liệu đến muộn của ngày cũ cần chạy lại `run-daily.sh --start ... --end ...`. ML hiện dùng một mô hình chung cho các trạm và đánh giá một holdout theo thời gian.
- Khi Docker không ra Internet, cần sửa cấu hình mạng/proxy của Docker hoặc truyền proxy qua `docker-compose build --build-arg HTTPS_PROXY=...`; runtime của dự án không phụ thuộc proxy tải thư viện.

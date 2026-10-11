# Kiểm tra môi trường Docker

## Phạm vi nghiệm thu

Khởi động công cụ bằng Docker Compose và kiểm tra cấu hình/kết nối. Không gửi dữ liệu mô phỏng, submit Spark job, trigger DAG hoặc chạy test full luồng.

| Hạng mục | Điều kiện đạt |
| --- | --- |
| Compose | `docker-compose config --quiet` thành công |
| Khởi tạo | kafka-init, database-migrate, airflow-init kết thúc mã 0 |
| Kafka | Healthy; topic sensor-events có 3 partitions |
| TimescaleDB | Healthy; schema có sẵn |
| Spark | Master/worker healthy; worker Alive trong UI master |
| Airflow | Webserver/scheduler healthy; không có DAG import error; DAG dự báo Paused |
| Grafana | API health OK; datasource TimescaleDB kết nối được; có 4 dashboard |
| Chế độ nền tảng | Producer/streaming/test không chạy; không có Spark application nghiệp vụ đang hoạt động |
| Lưu bền | Volumes được giữ khi chạy scripts/stop.sh |

## Lệnh kiểm tra thủ công

```bash
docker-compose config --quiet
docker-compose ps -a
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic sensor-events
docker-compose exec -T airflow-scheduler airflow dags list-import-errors -o json
docker-compose exec -T airflow-scheduler airflow dags list -o json
```

Mở Spark master tại http://localhost:8080, Airflow tại http://localhost:8088 và Grafana tại http://localhost:3000. Dashboard chưa có dữ liệu không phải lỗi nền tảng.

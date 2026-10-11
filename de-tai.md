
### Đề tài 4: Xây dựng hệ thống giám sát và dự báo ô nhiễm môi trường / giao thông thông minh từ cảm biến IoT (Smart City IoT Platform for Environmental & Traffic Forecasting)

- Mô tả bài toán: Tiếp nhận và phân tích hàng triệu gói tin chuỗi thời gian (timeseries) phát ra từ mạng lưới trạm cảm biến môi trường (chỉ số PM2.5, CO2, nhiệt
độ, độ ẩm) hoặc camera giám sát lưu lượng giao thông đô thị, từ đó đưa ra cảnh
báo sớm về ô nhiễm và ùn tắc giao thông.
- Công nghệ & Kiến trúc đề xuất:
    - Thu thập & Đệm dữ liệu: Apache Kafka tiếp nhận luồng dữ liệu cảm biến
    tần suất cao (>= 500 events/giây).
    - Xử lý luồng: Spark Streaming làm sạch dữ liệu nhiễu, thực hiện nội suy các
    điểm dữ liệu bị mất và tổng hợp dữ liệu theo cửa sổ thời gian
    (tumbling/sliding windows).
    - Tự động hóa & Điều phối luồng việc: Apache Airflow lập lịch tự động cho
    các tác vụ tổng hợp dữ liệu cuối ngày và kích hoạt pipeline huấn luyện lại
    mô hình phân tán.
- Thuật toán cốt lõi & Tính toán phân tán:
    - Huấn luyện mô hình hồi quy chuỗi thời gian phân tán (Linear Regression /
    Random Forest Regressor) trên Spark MLlib để dự báo nồng độ ô nhiễm
    hoặc mật độ phương tiện trong 1–3 giờ tới.
    - Kỹ thuật phát hiện ngoại lai (Anomaly Detection): Sử dụng độ lệch chuẩn
    phân tán (Rolling Z-score) để kích hoạt cảnh báo vượt ngưỡng khí thải
    nguy hại.
- Lưu trữ & Bảng điều khiển (Dashboard):
    - Lưu trữ chuỗi thời gian: Dữ liệu đã xử lý lưu vào CSDL chuyên dụng cho
    chuỗi thời gian (InfluxDB / Cassandra / TimescaleDB).
    - Trực quan hóa: Xây dựng bản đồ nhiệt (Heatmap) và bảng giám sát thời
    gian thực tương tác trực quan với Grafana.

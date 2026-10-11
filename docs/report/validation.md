# Quy trình nghiệm thu và thí nghiệm

## Phạm vi

Đối chiếu đề tài 4 trong `../de-tai.md` ở thư mục cha dự án. Chọn nhánh ô nhiễm môi trường, PM2.5 là biến dự báo. Mục tiêu một triệu bản tin và >=500 events/s cần số đo trên cấu hình máy được ghi rõ; không suy ra từ giá trị cấu hình producer.

## Kiểm thử tự động

`scripts/test.sh` kiểm tra nội suy/số âm/null, cách ly trạm, cửa sổ 30 phút loại điểm hiện tại, std=0, gap theo giờ, nhãn +1/+2/+3h, phân chia theo thời gian, retry cùng batch, baseline qua batch và cập nhật điểm đến muộn. Test database sử dụng một database tạm độc lập.

## Kịch bản tích hợp

1. `scripts/start.sh`; xác nhận Kafka/TimescaleDB/Spark healthy, migration exit 0, streaming không restart liên tục.
2. Gửi 7 ngày × 4 trạm dữ liệu synthetic; ghi thời gian bắt đầu/kết thúc, cấu hình RAM/CPU và file `experiments/throughput/latest.json`.
3. Đợi `sensor_raw` và `sensor_metrics` đạt 40.320 dòng. Kiểm tra `pm25_imputed`, `is_anomaly`, `is_threshold_exceeded` đều có giá trị true trong demo.
4. Kiểm tra cả `tumbling_5m` và `sliding_30m` trong `sensor_windows`; xem bốn dashboard và datasource health.
5. Chạy ML; cần 12 dự báo mới (4 trạm × 3 horizon), 6 dòng đánh giá (2 mô hình × 3 horizon) và model artifacts. Chép metrics bằng `scripts/export-results.sh`.
6. Chạy tổng hợp ngày, xác nhận số lượng và trung bình trong `daily_summary`. Kiểm tra DAG không có import error và hai task chạy đúng thứ tự.
7. Restart streaming khi đã xử lý hết; số khóa không đổi. Test tự động kiểm tra retry và đến muộn trên dữ liệu nhỏ xác định trước.

## Đo tốc độ

- Đầu vào: producer đếm số Kafka future được acknowledge; báo `acknowledged_events_per_second` sau flush. Cấu hình 500 events/s là tốc độ mục tiêu, số đo có thể thấp hơn do overhead; tăng mục tiêu lên 600/1000 để kiểm tra ngưỡng >=500.
- Xử lý: log mỗi batch chứa số event hợp lệ và `processed_events_per_second`; theo dõi backlog và thời gian từ lúc producer kết thúc đến khi số dòng sink đạt kỳ vọng. Không dùng tốc độ gửi Kafka thay thế throughput end-to-end.
- Triệu bản tin: `--synthetic --hours 168 --stations 100` tạo 1.008.000 events. Chạy khi có đủ tài nguyên; lưu log, thời gian, số dòng cuối cùng và biểu đồ throughput. Tăng số worker/partitions là một thí nghiệm riêng có ghi cấu hình.

## Đánh giá thuật toán

Synthetic có spike mỗi 137 phút và null mỗi 97 phút, giúp kiểm tra tính đúng. Muốn đánh giá phát hiện bất thường định lượng cần tập nhãn ground truth độc lập và precision/recall/F1; không gọi mọi giá trị cao là ground truth thực tế. Với dự báo, chạy dữ liệu Beijing, ghi MAE/RMSE/R² theo horizon; so sánh thêm persistence baseline và nhiều thời đoạn nếu viết phần nghiên cứu mở rộng.

## Kết quả kiểm chứng

Kết quả thực tế của lần chạy hoàn thiện được ghi riêng trong `verification.md`; phần này chỉ là quy trình, không phải bằng chứng hệ thống đã đạt tải mục tiêu.

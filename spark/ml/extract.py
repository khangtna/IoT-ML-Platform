from common import read_table


# Đọc dữ liệu cảm biến đã xử lý; as_of giới hạn lịch sử được phép sử dụng.
def read_sensor_metrics(spark, as_of=None):
    df = read_table(spark, "sensor_metrics")
    return df.filter(df.timestamp < as_of) if as_of is not None else df

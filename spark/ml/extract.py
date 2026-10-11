from common import read_table


def read_sensor_metrics(spark, as_of=None):
    df = read_table(spark, "sensor_metrics")
    return df.filter(df.timestamp < as_of) if as_of is not None else df

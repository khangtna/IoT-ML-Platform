"""Regression tests for time semantics, recovery and missing sensor values."""
import os
import unittest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F
from schemas import EVENT_SCHEMA
from transform import normalize, interpolate
from anomaly_detection import add_anomaly_features
from features import build_feature_frame, build_training_frame, temporal_split, latest_features


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("smartcity-tests")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self, values):
        start = datetime(2026, 1, 1)
        rows = [{"event_id": str(i), "station_id": station,
                 "timestamp": (start + timedelta(minutes=minute)).isoformat(), "pm25": value}
                for i, (station, minute, value) in enumerate(values)]
        return self.spark.createDataFrame(rows, EVENT_SCHEMA)

    def test_interpolation_cleaning_and_station_isolation(self):
        raw = self.events([("a", 0, 10.0), ("a", 5, -1.0), ("a", 10, 30.0),
                           ("b", 5, None), ("a", 500, None)])
        rows = interpolate(normalize(raw)).orderBy("station_id", "timestamp").collect()
        self.assertAlmostEqual(rows[1].pm25, 20.0)
        self.assertTrue(rows[1].pm25_imputed)
        self.assertIsNone(rows[3].pm25)
        self.assertIsNone(rows[4].pm25)

    def test_zscore_excludes_current_and_older_than_30_minutes(self):
        raw = self.events([("a", 0, 999.0), ("a", 40, 10.0), ("a", 41, 11.0),
                           ("a", 42, 9.0), ("a", 43, 100.0)])
        last = add_anomaly_features(normalize(raw)).orderBy(F.col("timestamp").desc()).first()
        self.assertAlmostEqual(last.pm25_rolling_mean_30m, 10.0)
        self.assertGreater(last.pm25_z_score, 100)
        self.assertTrue(last.is_anomaly)
        self.assertTrue(last.is_threshold_exceeded)

    def test_constant_baseline_has_no_invalid_zscore(self):
        raw = self.events([("a", i, 10.0) for i in range(4)])
        last = add_anomaly_features(normalize(raw)).orderBy(F.col("timestamp").desc()).first()
        self.assertIsNone(last.pm25_z_score)
        self.assertFalse(last.is_anomaly)

    def test_hourly_horizons_and_temporal_embargo(self):
        start = datetime(2026, 1, 1)
        df = self.spark.createDataFrame([(start + timedelta(hours=h), "a", float(h)) for h in range(100)],
                                        "timestamp timestamp, station_id string, pm25 double")
        features = build_feature_frame(df).cache()
        try:
            for horizon in (1, 2, 3):
                data = build_training_frame(features, horizon)
                row = data.orderBy("timestamp").first()
                self.assertEqual(row.label_time - row.timestamp, timedelta(hours=horizon))
                self.assertEqual(row.label - row.pm25, horizon)
                train, test = temporal_split(data)
                self.assertLessEqual(train.agg(F.max("label_time")).first()[0], test.agg(F.min("timestamp")).first()[0])
            self.assertEqual(latest_features(features).count(), 1)
        finally:
            features.unpersist()

    def test_latest_hour_respects_asof(self):
        start = datetime(2026, 1, 1)
        df = self.spark.createDataFrame([(start + timedelta(hours=h, minutes=59), "a", float(h)) for h in range(6)],
                                        "timestamp timestamp, station_id string, pm25 double")
        result = latest_features(build_feature_frame(df, as_of=start + timedelta(hours=6))).first()
        self.assertEqual(result.timestamp, start + timedelta(hours=6))
        self.assertEqual(result.pm25, 5.0)

    def test_missing_hour_is_not_previous_row(self):
        start = datetime(2026, 1, 1)
        df = self.spark.createDataFrame([(start + timedelta(hours=h), "a", float(h)) for h in (0,1,2,3,5,6,7,8,9,10)],
                                        "timestamp timestamp, station_id string, pm25 double")
        times = {r.timestamp for r in build_feature_frame(df).collect()}
        self.assertNotIn(start + timedelta(hours=6), times)
        self.assertIn(start + timedelta(hours=9), times)

    @unittest.skipUnless(os.getenv("TEST_DATABASE") == "1", "requires TimescaleDB and JDBC jar")
    def test_cross_batch_recovery_and_late_interpolation(self):
        import common
        from streaming_job import write_batch
        # Run against a disposable database, never the application's data tables.
        admin = common.connect()
        admin.autocommit = True
        name = "smartcity_test_" + str(os.getpid())
        previous = os.environ["JDBC_URL"]
        with admin.cursor() as cur:
            cur.execute('CREATE DATABASE "' + name + '"')
        os.environ["JDBC_URL"] = previous.rsplit("/", 1)[0] + "/" + name
        try:
            with common.connect() as conn, conn.cursor() as cur:
                with open("/opt/project/database/init.sql") as schema:
                    cur.execute(schema.read())
            first = self.events([("a",0,10.0),("a",5,None),("a",10,30.0)])
            write_batch(first,0)
            write_batch(first,0)  # Retry the same batch: no duplicate keys.
            second = self.events([("a",15,20.0),("a",20,100.0)])
            write_batch(second,1)
            with common.connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM sensor_metrics")
                self.assertEqual(cur.fetchone()[0],5)
                cur.execute("SELECT pm25,pm25_imputed FROM sensor_metrics WHERE timestamp='2026-01-01 00:05:00+00'")
                self.assertEqual(cur.fetchone(), (20.0,True))
                cur.execute("SELECT pm25_rolling_mean_30m,is_anomaly FROM sensor_metrics WHERE timestamp='2026-01-01 00:20:00+00'")
                baseline, anomaly = cur.fetchone()
                self.assertAlmostEqual(baseline,20.0)
                self.assertTrue(anomaly)
            write_batch(self.events([("a",10,50.0)]),2)  # Late correction.
            with common.connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT pm25 FROM sensor_metrics WHERE timestamp='2026-01-01 00:05:00+00'")
                self.assertEqual(cur.fetchone()[0],30.0)
        finally:
            os.environ["JDBC_URL"] = previous
            with admin.cursor() as cur:
                cur.execute('DROP DATABASE "' + name + '" WITH (FORCE)')
            admin.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

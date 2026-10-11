"""Replay chronologically merged Beijing CSVs or deterministic demo sensors."""
import argparse
import csv
import heapq
import json
import math
import os
import random
import time
from collections import deque
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

NULL = {"", "NA", "NaN", "nan", None}


def nf(value):
    if value in NULL:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def event(row):
    timestamp = datetime(int(row["year"]), int(row["month"]), int(row["day"]),
                         int(row["hour"]), tzinfo=timezone(timedelta(hours=8))).isoformat()
    fields = {"pm25": "PM2.5", "pm10": "PM10", "so2": "SO2", "no2": "NO2", "co": "CO",
              "o3": "O3", "temperature": "TEMP", "pressure": "PRES", "dew_point": "DEWP",
              "rainfall": "RAIN", "wind_speed": "WSPM"}
    return {"event_id": f"{row['station']}-{timestamp}", "station_id": row["station"],
            "timestamp": timestamp, **{k: nf(row.get(v)) for k, v in fields.items()},
            "wind_direction": None if row.get("wd") in NULL else row["wd"]}


def csv_events(directory):
    files = sorted(Path(directory).glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files in {directory}; use --synthetic for demo data")
    def ordered(reader):
        previous = ""
        for row in reader:
            item = event(row)
            if item["timestamp"] < previous:
                raise ValueError("Each Beijing CSV must be sorted chronologically")
            previous = item["timestamp"]
            yield item
    with ExitStack() as stack:
        readers = [ordered(csv.DictReader(stack.enter_context(f.open(encoding="utf-8-sig", newline="")))) for f in files]
        yield from heapq.merge(*readers, key=lambda item: (item["timestamp"], item["station_id"]))


def synthetic_events(hours=168, stations=4, start=None):
    rng = random.Random(42)
    start = start or (datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours))
    for minute in range(hours * 60):
        timestamp = (start + timedelta(minutes=minute)).isoformat()
        for station in range(stations):
            pm25 = max(1, 35 + station * 4 + 18 * math.sin(minute / 1440 * 2 * math.pi) + rng.gauss(0, 2))
            if minute % 137 == 100:
                pm25 += 150  # Known spikes for anomaly demonstrations.
            if minute % 97 == 50:
                pm25 = None  # Bounded missing values for interpolation.
            name = f"demo-{station + 1:02d}"
            yield {"event_id": f"{name}-{timestamp}", "station_id": name, "timestamp": timestamp,
                "pm25": pm25, "pm10": 60.0, "so2": 8.0, "no2": 25.0, "co": 600.0, "o3": 50.0,
                "temperature": 24 + 4 * math.sin(minute / 1440 * 2 * math.pi), "pressure": 1010.0,
                "dew_point": 15.0, "rainfall": 0.0, "wind_speed": 2.0, "wind_direction": "NE"}


def main():
    from kafka import KafkaProducer
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/opt/project/data/raw/beijing-multisite")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--hours", type=int, default=168)
    parser.add_argument("--stations", type=int, default=4)
    parser.add_argument("--events", type=int, default=0, help="0 = all source events")
    parser.add_argument("--events-per-second", type=int, default=int(os.getenv("EVENTS_PER_SECOND", "500")))
    args = parser.parse_args()
    if args.events_per_second <= 0 or args.hours <= 0 or args.stations <= 0 or args.events < 0:
        parser.error("rate, hours and stations must be positive; events >= 0")
    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        key_serializer=lambda value: value.encode(), value_serializer=lambda value: json.dumps(value, allow_nan=False).encode(),
        acks="all", linger_ms=10, batch_size=32768, retries=5)
    events = synthetic_events(args.hours, args.stations) if args.synthetic else csv_events(args.data_dir)
    pending = deque()
    sent = acknowledged = 0
    started = time.monotonic()
    try:
        for item in events:
            pending.append(producer.send(os.getenv("KAFKA_TOPIC", "sensor-events"), key=item["station_id"], value=item))
            sent += 1
            if len(pending) >= 2000:
                pending.popleft().get(timeout=60)
                acknowledged += 1
            delay = sent / args.events_per_second - (time.monotonic() - started)
            if delay > 0:
                time.sleep(delay)
            if args.events and sent >= args.events:
                break
        producer.flush(timeout=60)
        while pending:
            pending.popleft().get(timeout=60)
            acknowledged += 1
    finally:
        producer.close(timeout=30)
    elapsed = time.monotonic() - started
    report = {"sent": sent, "acknowledged": acknowledged, "seconds": elapsed,
              "acknowledged_events_per_second": acknowledged / elapsed,
              "target_events_per_second": args.events_per_second, "synthetic": args.synthetic}
    target = Path("/opt/project/experiments/throughput/latest.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()

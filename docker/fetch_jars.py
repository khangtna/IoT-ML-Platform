"""Install fixed JDBC dependencies; Kafka dependencies are commented until integration."""
import hashlib
import sys
import urllib.request
from pathlib import Path

COORDINATES = [
    # Member 1: bỏ comment nhóm Kafka và thư viện đi kèm khi bật lại streaming.
    # ('org.apache.spark', 'spark-sql-kafka-0-10_2.12', '3.5.3'),
    # ('org.apache.spark', 'spark-token-provider-kafka-0-10_2.12', '3.5.3'),
    # ('org.apache.kafka', 'kafka-clients', '3.4.1'),
    # ('org.apache.commons', 'commons-pool2', '2.11.1'),
    # ('com.google.code.findbugs', 'jsr305', '3.0.0'),
    # JDBC PostgreSQL dùng để Spark đọc TimescaleDB.
    ('org.postgresql', 'postgresql', '42.7.4'),
    ('org.checkerframework', 'checker-qual', '3.42.0'),
]
if len(sys.argv) > 1:
    target = Path(sys.argv[1])
else:
    import pyspark
    target = Path(pyspark.__file__).parent / 'jars'
for group, artifact, version in COORDINATES:
    name = f'{artifact}-{version}.jar'
    if (target / name).exists():
        continue
    url = f'https://repo.maven.apache.org/maven2/{group.replace(".", "/")}/{artifact}/{version}/{name}'
    with urllib.request.urlopen(url, timeout=120) as response:
        data = response.read()
    with urllib.request.urlopen(url + '.sha1', timeout=30) as response:
        expected = response.read().decode().strip().split()[0]
    if hashlib.sha1(data).hexdigest() != expected:
        raise ValueError(f'Maven checksum mismatch: {name}')
    (target / name).write_bytes(data)
    print('Installed', name, flush=True)

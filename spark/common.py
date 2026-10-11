"""Shared JDBC reads and retry-safe PostgreSQL writes for Spark jobs."""
import os
from urllib.parse import urlparse


def db_options():
    return {"url": os.getenv("JDBC_URL", "jdbc:postgresql://timescaledb:5432/smartcity"),
            "user": os.getenv("POSTGRES_USER", "smartcity"),
            "password": os.getenv("POSTGRES_PASSWORD", "smartcity"),
            "driver": "org.postgresql.Driver"}


def connect():
    import psycopg2
    options = db_options()
    url = urlparse(options["url"].replace("jdbc:", "", 1))
    return psycopg2.connect(host=url.hostname, port=url.port or 5432,
        dbname=url.path.lstrip("/"), user=options["user"], password=options["password"],
        options="-c timezone=UTC")


def read_table(spark, table):
    return spark.read.format("jdbc").options(**db_options()).option("dbtable", table).load()


def upsert(df, table, keys):
    """Write on executors; retries overwrite the same business keys."""
    from functools import partial
    df.repartition(int(os.getenv("DB_WRITE_PARTITIONS", "2")), *keys).foreachPartition(
        partial(_write_partition, table=table, columns=df.columns, keys=keys, options=db_options()))


def _write_partition(rows, table, columns, keys, options):
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    from itertools import islice
    url = urlparse(options["url"].replace("jdbc:", "", 1))
    statement = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
        sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, columns)),
        sql.SQL(",").join(map(sql.Identifier, keys)),
        sql.SQL(",").join(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                          for c in columns if c not in keys))
    with psycopg2.connect(host=url.hostname, port=url.port or 5432, dbname=url.path.lstrip("/"),
        user=options["user"], password=options["password"], options="-c timezone=UTC") as conn:
        with conn.cursor() as cur:
            while True:
                batch = list(islice(rows, 1000))
                if not batch:
                    break
                execute_values(cur, statement.as_string(conn), [tuple(row) for row in batch])

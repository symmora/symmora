#!/usr/bin/env python3
import argparse
import os
import sys
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


def merge_jdbc_url_params(base_url: str, extra_params: dict) -> str:
    """Merge extra_params into JDBC URL, preserving existing parameters.

    Works with URLs like:
      jdbc:clickhouse://host:8443/db?param=value
    """
    # For non-standard schemes like jdbc:clickhouse, urlparse won't populate params well,
    # but query string is still parseable.
    parsed = urlparse(base_url)
    query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_pairs.update({k: str(v) for k, v in extra_params.items() if v is not None})

    new_query = urlencode(query_pairs)
    new_parsed = parsed._replace(query=new_query)
    return urlunparse(new_parsed)


def build_spark(app_name: str, yarn_queue: str | None, extra_confs: list[tuple[str, str]]) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("yarn")
        .enableHiveSupport()
        # Sensible defaults; can be overridden via --conf
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if yarn_queue:
        builder = builder.config("spark.yarn.queue", yarn_queue)

    for k, v in extra_confs:
        builder = builder.config(k, v)

    return builder.getOrCreate()


def read_from_impala(spark: SparkSession, table: str | None, query: str | None):
    if (table is None) == (query is None):
        raise ValueError("Provide exactly one of --impala-table or --impala-query")

    try:
        if query is not None:
            return spark.sql(query)
        return spark.table(table)
    except AnalysisException as exc:
        raise SystemExit(f"Failed to read from Impala/Hive: {exc}")


def write_to_clickhouse(
    df,
    jdbc_url: str,
    table: str,
    user: str | None,
    password: str | None,
    ca_path: str,
    mode: str,
    batch_size: int,
    isolation_level: str,
    num_partitions: int | None,
):
    # Ensure CA file exists
    if not os.path.isfile(ca_path):
        raise SystemExit(f"CA file not found: {ca_path}")

    # Add TLS options for ClickHouse JDBC
    jdbc_url = merge_jdbc_url_params(
        jdbc_url,
        {
            "ssl": "true",
            # STRICT verifies server cert chain against provided CA; adjust if needed
            "sslmode": "STRICT",
            "sslrootcert": os.path.abspath(ca_path),
        },
    )

    # Writer options
    options = {
        "url": jdbc_url,
        "dbtable": table,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "batchsize": str(batch_size),
        "sessionInitStatement": "SET max_execution_time=0",
        "isolationLevel": isolation_level,
    }

    if user:
        options["user"] = user
    if password:
        options["password"] = password

    writer = df.write.format("jdbc").options(**options)

    if num_partitions and num_partitions > 0:
        df = df.repartition(num_partitions)
        writer = df.write.format("jdbc").options(**options)

    if mode not in ("append", "overwrite"):
        raise SystemExit("--write-mode must be either 'append' or 'overwrite'")

    # For overwrite, Spark JDBC uses CREATE TABLE + INSERT when supported; for ClickHouse,
    # prefer inserting into an existing table. We'll let mode control behavior.
    writer.mode(mode).save()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate a table from Impala (via Spark SQL/Hive) to ClickHouse using Spark on YARN."
        )
    )

    source = parser.add_argument_group("Source (Impala/Hive)")
    source.add_argument("--impala-table", help="Source table in 'db.table' format")
    source.add_argument(
        "--impala-query",
        help="Custom SQL query to read from Impala/Hive (use instead of --impala-table)",
    )

    sink = parser.add_argument_group("Target (ClickHouse)")
    sink.add_argument(
        "--clickhouse-url",
        required=True,
        help=(
            "ClickHouse JDBC URL, e.g. jdbc:clickhouse://host:8443/db. TLS params will be appended."
        ),
    )
    sink.add_argument(
        "--clickhouse-table",
        required=True,
        help=(
            "Target ClickHouse table name (recommend a Distributed table when targeting a cluster)"
        ),
    )
    sink.add_argument("--clickhouse-user", help="ClickHouse username")
    sink.add_argument("--clickhouse-password", help="ClickHouse password")
    sink.add_argument(
        "--ca-path",
        default="./CA.pem",
        help="Path to CA.pem used to verify ClickHouse server certificate (default: ./CA.pem)",
    )

    perf = parser.add_argument_group("Performance & behavior")
    perf.add_argument(
        "--write-mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode for ClickHouse (default: append)",
    )
    perf.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="JDBC batch size for ClickHouse inserts (default: 10000)",
    )
    perf.add_argument(
        "--num-partitions",
        type=int,
        default=None,
        help=(
            "Repartition the DataFrame to this number of partitions before write; tune for parallelism"
        ),
    )
    perf.add_argument(
        "--isolation-level",
        default="NONE",
        help="JDBC isolation level (ClickHouse typically uses NONE)",
    )

    sparkgrp = parser.add_argument_group("Spark & YARN")
    sparkgrp.add_argument(
        "--app-name", default="impala-to-clickhouse-migration", help="Spark application name"
    )
    sparkgrp.add_argument("--yarn-queue", help="YARN queue to submit the job to")
    sparkgrp.add_argument(
        "--conf",
        action="append",
        default=[],
        help=(
            "Additional Spark config in key=value form. Can be specified multiple times."
        ),
    )

    args = parser.parse_args(argv)

    # Parse --conf
    extra_confs: list[tuple[str, str]] = []
    for entry in args.conf:
        if "=" not in entry:
            raise SystemExit(f"Invalid --conf entry '{entry}', expected key=value")
        k, v = entry.split("=", 1)
        extra_confs.append((k.strip(), v.strip()))
    args.conf = extra_confs

    return args


def main(argv: list[str]) -> None:
    args = parse_args(argv)

    spark = build_spark(args.app_name, args.yarn_queue, args.conf)

    df = read_from_impala(spark, args.impala_table, args.impala_query)

    # Optional: show schema to logs
    df.printSchema()

    # Write to ClickHouse
    write_to_clickhouse(
        df=df,
        jdbc_url=args.clickhouse_url,
        table=args.clickhouse_table,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        ca_path=args.ca_path,
        mode=args.write_mode,
        batch_size=args.batch_size,
        isolation_level=args.isolation_level,
        num_partitions=args.num_partitions,
    )

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
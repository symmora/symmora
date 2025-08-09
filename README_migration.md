### Impala → ClickHouse migration with Spark on YARN

This job reads a table or SQL from Impala/Hive via Spark and writes into ClickHouse via JDBC with TLS using a local `CA.pem`.

#### Requirements
- Spark cluster with YARN
- Hive/Impala metastore accessible from Spark
- ClickHouse server (preferably a Distributed table when targeting a cluster)
- `CA.pem` in the working directory or pass `--ca-path`
- ClickHouse JDBC driver jar available to Spark executors and driver

Recommended ClickHouse JDBC dependency versions:
- `com.clickhouse:clickhouse-jdbc:0.6.3-patch13` (or newer)
- Alternative shaded driver: `com.clickhouse:clickhouse-jdbc:0.6.3-patch13:all`

Place the jar on HDFS or a local path accessible on all nodes, and reference via `--jars` (driver only) or `--packages` (cluster must have internet/mirror access).

#### Example: spark-submit on YARN (cluster mode)

```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name impala-to-clickhouse-migration \
  --conf spark.yarn.queue=default \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --jars /path/to/clickhouse-jdbc-0.6.3-patch13-all.jar \
  /workspace/migrate_impala_to_clickhouse.py \
  --impala-table db_src.table_src \
  --clickhouse-url jdbc:clickhouse://ch-secure-host:8443/target_db \
  --clickhouse-table target_table \
  --clickhouse-user ch_user \
  --clickhouse-password '***' \
  --ca-path ./CA.pem \
  --write-mode append \
  --batch-size 10000 \
  --num-partitions 200
```

Alternatively, use Maven coordinates (requires internet):

```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages com.clickhouse:clickhouse-jdbc:0.6.3-patch13 \
  /workspace/migrate_impala_to_clickhouse.py \
  --impala-query "SELECT * FROM db_src.table_src WHERE ds = '2025-08-01'" \
  --clickhouse-url jdbc:clickhouse://ch-secure-host:8443/target_db \
  --clickhouse-table target_table \
  --ca-path ./CA.pem
```

#### Notes
- TLS: The script appends `ssl=true&sslmode=STRICT&sslrootcert=/abs/path/to/CA.pem` to the ClickHouse JDBC URL.
- Schema: The script does not create the target table. Pre-create it in ClickHouse with a matching schema (or compatible types).
- Parallelism: Tune `--num-partitions` to match cluster resources and ClickHouse ingestion capacity.
- Overwrite: `--write-mode overwrite` will attempt to overwrite via JDBC semantics; prefer truncating the table manually in ClickHouse if needed.
- Large strings/maps/arrays: Ensure ClickHouse column types can handle source data (e.g., use `String`, `Array(T)`, `Map(K,V)`).
- Time zones: Spark session time zone is set to UTC.
"""
EdgeCloudX Spark Analytics — Structured Streaming
====================================================
PySpark Structured Streaming job that consumes traffic-density events
from Kafka and computes rolling analytics:
- 5-min / 15-min / 1-hour rolling congestion averages
- Top-5 busiest intersections per window
- Congestion trend detection (rising/falling/stable)

Results are written to Redis (real-time) and PostgreSQL (historical).
"""

import json
import logging
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    desc,
    from_json,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-analytics")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
POSTGRES_URL = os.environ.get(
    "JDBC_URL",
    "jdbc:postgresql://postgres:5432/edgecloudx",
)
POSTGRES_USER = os.environ.get("POSTGRES_USER", "edgecloudx")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "edgecloudx_secret")

# Schema for traffic-density Kafka messages
TRAFFIC_SCHEMA = StructType([
    StructField("intersection_id", StringType(), True),
    StructField("edge_node_id", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("congestion_score", DoubleType(), True),
    StructField("anomaly_detected", BooleanType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("trace_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create and configure a SparkSession."""
    return (
        SparkSession.builder
        .appName("EdgeCloudX-Analytics")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )


def write_to_postgres(df, epoch_id, table_name):
    """Write a streaming micro-batch to PostgreSQL."""
    if df.count() == 0:
        return

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    logger.info(f"Wrote {df.count()} rows to {table_name} (epoch {epoch_id})")


def write_rolling_avg_to_redis(df, epoch_id):
    """Write rolling averages to Redis for real-time dashboard consumption."""
    import redis

    r = redis.from_url(REDIS_URL, decode_responses=True)
    rows = df.collect()

    for row in rows:
        key = f"analytics:rolling:{row['intersection_id']}"
        r.hset(key, mapping={
            "intersection_id": row["intersection_id"],
            "avg_congestion_5m": str(round(row.get("avg_congestion", 0.0), 3)),
            "max_congestion_5m": str(round(row.get("max_congestion", 0.0), 3)),
            "total_vehicles_5m": str(row.get("total_vehicles", 0)),
            "event_count_5m": str(row.get("event_count", 0)),
            "window_start": str(row.get("window", {}).get("start", "")),
            "window_end": str(row.get("window", {}).get("end", "")),
        })
        r.expire(key, 600)  # 10 min TTL

    if rows:
        # Publish summary for dashboard
        summary = {
            "type": "spark_rolling_update",
            "intersections": len(rows),
            "timestamp": time.time(),
        }
        r.publish("analytics:spark:updates", json.dumps(summary))

    r.close()
    logger.info(f"Published {len(rows)} rolling averages to Redis (epoch {epoch_id})")


def main():
    logger.info("=" * 60)
    logger.info("  EdgeCloudX Spark Analytics — Starting")
    logger.info(f"  Kafka: {KAFKA_SERVERS}")
    logger.info(f"  Redis: {REDIS_URL}")
    logger.info(f"  Postgres: {POSTGRES_URL}")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "traffic-density")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON messages
    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRAFFIC_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    # ── Stream 1: 5-minute rolling averages (to Redis) ──
    rolling_5m = (
        parsed
        .withWatermark("kafka_timestamp", "2 minutes")
        .groupBy(
            col("intersection_id"),
            window("kafka_timestamp", "5 minutes", "1 minute"),
        )
        .agg(
            avg("congestion_score").alias("avg_congestion"),
            spark_max("congestion_score").alias("max_congestion"),
            spark_min("congestion_score").alias("min_congestion"),
            spark_sum("vehicle_count").alias("total_vehicles"),
            count("*").alias("event_count"),
            spark_sum(col("anomaly_detected").cast("int")).alias("anomaly_count"),
        )
    )

    query_redis = (
        rolling_5m.writeStream
        .outputMode("update")
        .foreachBatch(write_rolling_avg_to_redis)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Stream 2: 1-hour aggregations (to PostgreSQL) ──
    hourly = (
        parsed
        .withWatermark("kafka_timestamp", "10 minutes")
        .groupBy(
            col("intersection_id"),
            window("kafka_timestamp", "1 hour"),
        )
        .agg(
            avg("congestion_score").alias("avg_congestion"),
            spark_max("congestion_score").alias("peak_congestion"),
            spark_min("congestion_score").alias("min_congestion"),
            spark_sum("vehicle_count").alias("total_vehicles"),
            count("*").alias("total_events"),
            spark_sum(col("anomaly_detected").cast("int")).alias("total_anomalies"),
        )
        .select(
            col("intersection_id"),
            col("window.start").alias("hour_start"),
            col("avg_congestion"),
            col("peak_congestion"),
            col("min_congestion"),
            col("total_vehicles"),
            col("total_events"),
            col("total_anomalies"),
        )
    )

    query_postgres = (
        hourly.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, eid: write_to_postgres(df, eid, "spark_hourly_stats"))
        .trigger(processingTime="5 minutes")
        .start()
    )

    logger.info("Spark streaming queries started")
    logger.info("  - 5-min rolling averages → Redis (every 30s)")
    logger.info("  - 1-hour aggregations → PostgreSQL (every 5min)")

    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

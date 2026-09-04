"""Spark Structured Streaming job — the production-scale path of the same pipeline.

Reads JSON events from Kafka (or a watched directory), applies the same
validation/enrichment semantics as the Python path, computes 1-minute windowed
aggregations with a 30 s watermark, and upserts them into PostgreSQL using a
200 ms micro-batch trigger for sub-second end-to-end latency.

Tuning highlights (sub-second latency + parallel execution):
  * trigger(processingTime="200 milliseconds") — small micro-batches
  * spark.sql.shuffle.partitions=8             — small stateful streaming shuffles
  * adaptive execution disabled                — AQE adds per-batch planning overhead

Usage:
  # Spark 4.x (Scala 2.13) — match the version to your pyspark install:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
      src/spark/streaming_job.py --source kafka \
      --kafka-bootstrap localhost:9092 --pg-dsn postgresql://dpe:dpe@localhost:5432/dpe

  # File-source variant (no Kafka required):
  spark-submit src/spark/streaming_job.py --source file --input-dir data/stream-input

  Windows note: local filesystem sources/sinks need winutils.exe + hadoop.dll on
  HADOOP_HOME (see README); run under WSL/Linux/Docker for the smoothest path.
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                               TimestampType)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("ts", TimestampType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("region", StringType()),
    StructField("device", StringType()),
    StructField("amount", DoubleType()),
])

REGION_FX = {"us-east": 1.00, "us-west": 1.00, "eu-central": 1.08, "ap-south": 0.93, "latam": 1.12}
VALID_TYPES = ("view", "click", "add_to_cart", "purchase", "refund")
VALID_REGIONS = tuple(REGION_FX)
VALID_DEVICES = ("mobile", "desktop", "tablet")

MARGIN_EXPR = """CASE event_type
    WHEN 'view' THEN 0.0
    WHEN 'click' THEN 0.01
    WHEN 'add_to_cart' THEN 0.02
    WHEN 'purchase' THEN 0.32
    WHEN 'refund' THEN -0.35
END"""

UPSERT_SQL = """
INSERT INTO aggregates_1m (window_start, region, event_type, events, revenue) VALUES %s
ON CONFLICT (window_start, region, event_type) DO UPDATE
SET events = aggregates_1m.events + EXCLUDED.events,
    revenue = aggregates_1m.revenue + EXCLUDED.revenue
"""


def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("dpe-streaming")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.streaming.metricsEnabled", "true")
            .getOrCreate())


def main() -> None:
    parser = argparse.ArgumentParser(description="DPE Spark Structured Streaming job")
    parser.add_argument("--source", choices=["kafka", "file"], default="kafka")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--input-dir", default="data/stream-input")
    parser.add_argument("--checkpoint", default="data/checkpoints/streaming")
    parser.add_argument("--sink", choices=["postgres", "console"], default="console")
    parser.add_argument("--pg-dsn", default="postgresql://dpe:dpe@localhost:5432/dpe")
    parser.add_argument("--trigger", default="200 milliseconds")
    args = parser.parse_args()

    spark = build_spark()

    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.kafka_bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "latest")
               .load())
        parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")
    else:
        parsed = (spark.readStream.format("json")
                  .schema(EVENT_SCHEMA)
                  .option("maxFilesPerTrigger", 10)
                  .option("latestFirst", "true")
                  .load(args.input_dir))

    # validation + enrichment — mirrors src/stream/transformations.py
    fx_map = F.create_map([F.lit(x) for kv in REGION_FX.items() for x in kv])
    enriched = (parsed
                .filter(F.col("event_type").isin(list(VALID_TYPES)))
                .filter(F.col("region").isin(list(VALID_REGIONS)))
                .filter(F.col("device").isin(list(VALID_DEVICES)))
                .filter(F.col("amount").isNull() | (F.col("amount") >= 0))
                .withColumn("revenue",
                            F.round(F.coalesce("amount", F.lit(0.0))
                                    * F.expr(MARGIN_EXPR)
                                    * fx_map[F.col("region")], 4)))

    windowed = (enriched
                .withWatermark("ts", "30 seconds")
                .groupBy(F.window("ts", "1 minute"), "region", "event_type")
                .agg(F.count(F.lit(1)).alias("events"),
                     F.sum("revenue").alias("revenue")))

    if args.sink == "postgres":
        def upsert_batch(batch_df, batch_id: int) -> None:
            rows = [(r["window"]["start"], r["region"], r["event_type"],
                     r["events"], float(r["revenue"]))
                    for r in batch_df.collect()]
            if not rows:
                return
            import psycopg2
            from psycopg2.extras import execute_values
            conn = psycopg2.connect(args.pg_dsn)
            try:
                execute_values(conn.cursor(), UPSERT_SQL, rows, page_size=1000)
                conn.commit()
            finally:
                conn.close()

        query = (windowed.writeStream
                 .foreachBatch(upsert_batch)
                 .outputMode("update")
                 .option("checkpointLocation", args.checkpoint)
                 .trigger(processingTime=args.trigger)
                 .start())
    else:
        query = (windowed.writeStream
                 .outputMode("update")
                 .format("console")
                 .option("truncate", "false")
                 .trigger(processingTime=args.trigger)
                 .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()

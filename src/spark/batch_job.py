"""Spark batch job — nightly aggregation + insights at cluster scale.

Reads the events table over JDBC with a partitioned (parallel) scan, rebuilds
the hourly summary table, and prints insight-style analytics (top regions,
refund rate). Also suitable for refreshing the mv_region_daily materialized
view that the query router serves heavy aggregates from.

Usage:
  spark-submit --packages org.postgresql:postgresql:42.7.3 \
      src/spark/batch_job.py \
      --pg-url jdbc:postgresql://localhost:5432/dpe \
      --pg-user dpe --pg-password dpe \
      --start "2026-09-03 00:00:00" --end "2026-09-04 00:00:00" --partitions 8

  Windows note: JDBC reads still touch the local temp dirs — winutils.exe on
  HADOOP_HOME is required (see README); WSL/Linux/Docker recommended.
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("dpe-batch")
            .config("spark.sql.shuffle.partitions", "16")
            .getOrCreate())


def main() -> None:
    parser = argparse.ArgumentParser(description="DPE Spark batch aggregation job")
    parser.add_argument("--pg-url", default="jdbc:postgresql://localhost:5432/dpe")
    parser.add_argument("--pg-user", default="dpe")
    parser.add_argument("--pg-password", default="dpe")
    parser.add_argument("--start", required=True, help="inclusive, e.g. '2026-09-03 00:00:00'")
    parser.add_argument("--end", required=True, help="exclusive")
    parser.add_argument("--partitions", type=int, default=8, help="parallel JDBC read partitions")
    args = parser.parse_args()

    spark = build_spark()

    pushdown = (f"(SELECT * FROM events WHERE ts >= '{args.start}' AND ts < '{args.end}') AS e")
    reader = (spark.read.format("jdbc")
              .option("url", args.pg_url)
              .option("dbtable", pushdown)
              .option("user", args.pg_user)
              .option("password", args.pg_password))
    # Timestamps are ns-resolution strings in JDBC; partition on a numeric surrogate.
    # For simplicity we hash-split on event_id when a numeric partition column is absent.
    df = (reader.option("partitionColumn", "abs(hashtext(event_id))")
                .option("lowerBound", "-2147483648")
                .option("upperBound", "2147483647")
                .option("numPartitions", str(args.partitions))
                .load())

    summary = (df.groupBy(F.date_trunc("hour", F.col("ts")).alias("window_start"),
                          "region", "event_type")
                 .agg(F.count(F.lit(1)).alias("events"),
                      F.sum("revenue").alias("revenue")))

    (summary.write.format("jdbc")
     .option("url", args.pg_url)
     .option("dbtable", "summary_hourly")
     .option("user", args.pg_user)
     .option("password", args.pg_password)
     .mode("append")
     .save())

    # Insight-style analytics at scale
    top_region = (df.groupBy("region").agg(F.sum("revenue").alias("revenue"))
                    .orderBy(F.desc("revenue")).first())
    total = df.count()
    refunds = df.filter(F.col("event_type") == "refund").count()
    purchases = df.filter(F.col("event_type") == "purchase").count()
    print(f"[batch] window {args.start} -> {args.end}: {total} events")
    if top_region:
        print(f"[batch] top region by revenue: {top_region['region']} ({top_region['revenue']})")
    if purchases:
        print(f"[batch] refund rate: {refunds / purchases:.2%}")
    spark.stop()


if __name__ == "__main__":
    main()

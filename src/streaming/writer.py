"""Reusable Structured Streaming sink builders."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence


def write_parquet_stream(
    dataframe,
    *,
    path: Path,
    checkpoint: Path,
    partition_by: Sequence[str],
    output_mode: str = "append",
    query_name: str,
    trigger_interval: str = "10 seconds",
):
    return (
        dataframe.writeStream.format("parquet")
        .queryName(query_name)
        .outputMode(output_mode)
        .option("path", str(path))
        .option("checkpointLocation", str(checkpoint))
        .partitionBy(*partition_by)
        .trigger(processingTime=trigger_interval)
        .start()
    )


def write_json_kafka_stream(
    dataframe,
    *,
    bootstrap_servers: str,
    topic: str,
    checkpoint: Path,
    query_name: str,
    output_mode: str = "append",
):
    from pyspark.sql import functions as F

    encoded = dataframe.select(
        F.col("event_id").cast("string").alias("key")
        if "event_id" in dataframe.columns
        else F.lit(None).cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(name) for name in dataframe.columns])).alias("value"),
    )
    return (
        encoded.writeStream.format("kafka")
        .queryName(query_name)
        .outputMode(output_mode)
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", str(checkpoint))
        .start()
    )

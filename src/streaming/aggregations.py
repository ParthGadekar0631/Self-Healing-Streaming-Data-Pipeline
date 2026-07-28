"""Stateful event-time Structured Streaming aggregations."""

from __future__ import annotations


def build_aggregations(dataframe, window_duration: str = "5 minutes"):
    from pyspark.sql import functions as F

    return (
        dataframe.groupBy(
            F.window("event_timestamp", window_duration).alias("event_window"),
            "event_type",
            "region",
            "source_system",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.avg("event_value").alias("average_event_value"),
            F.sum(F.when(F.col("event_type") == "error_event", 1).otherwise(0)).alias(
                "error_event_count"
            ),
            F.approx_count_distinct("device_id").alias("unique_device_count"),
        )
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "event_type",
            "region",
            "source_system",
            "event_count",
            "average_event_value",
            "error_event_count",
            "unique_device_count",
            F.to_date(F.col("event_window.start")).alias("window_date"),
        )
    )

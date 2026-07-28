"""Analytics-ready transformations for Python records and Spark DataFrames."""

from __future__ import annotations

import hashlib
from typing import Any

from src.utils.time_utils import parse_timestamp, utc_now_iso


def transform_event(event: dict[str, Any]) -> dict[str, Any]:
    transformed = dict(event)
    timestamp = parse_timestamp(str(event["event_timestamp"]))
    transformed["event_timestamp"] = timestamp.isoformat()
    transformed["event_date"] = timestamp.date().isoformat()
    transformed["event_type"] = str(event["event_type"]).lower()
    transformed["region"] = str(event["region"]).lower()
    transformed["event_value"] = (
        float(event["event_value"]) if event.get("event_value") is not None else None
    )
    transformed["processed_at"] = utc_now_iso()
    transformed["processing_key"] = hashlib.sha256(
        f"{event['event_id']}|{event['payload_version']}".encode()
    ).hexdigest()
    return transformed


def transform_dataframe(dataframe):
    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("event_type", F.lower("event_type"))
        .withColumn("region", F.lower("region"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "processing_key",
            F.sha2(F.concat_ws("|", F.col("event_id"), F.col("payload_version")), 256),
        )
    )

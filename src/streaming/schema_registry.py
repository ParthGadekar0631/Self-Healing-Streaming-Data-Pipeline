"""Canonical Pydantic and Spark event schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


ALLOWED_EVENT_TYPES = {
    "page_view",
    "click",
    "purchase",
    "sensor_reading",
    "error_event",
    "heartbeat",
}
ALLOWED_REGIONS = {"us-east", "us-west", "eu-west", "ap-south"}
SUPPORTED_PAYLOAD_VERSIONS = {"1.0", "1.1"}


class EventRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str
    user_id: str | None = None
    device_id: str | None = None
    event_type: str
    event_timestamp: str
    ingestion_timestamp: str
    source_system: str
    region: str
    session_id: str | None = None
    event_value: float | None = None
    event_status: str = "accepted"
    payload_version: str
    metadata: dict[str, Any] = Field(min_length=1)


def spark_event_schema():
    """Build lazily so pure unit tests do not need to initialize Spark."""
    from pyspark.sql.types import (
        DoubleType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
            StructField("device_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_timestamp", StringType()),
            StructField("ingestion_timestamp", StringType()),
            StructField("source_system", StringType()),
            StructField("region", StringType()),
            StructField("session_id", StringType()),
            StructField("event_value", DoubleType()),
            StructField("event_status", StringType()),
            StructField("payload_version", StringType()),
            StructField("metadata", MapType(StringType(), StringType())),
        ]
    )

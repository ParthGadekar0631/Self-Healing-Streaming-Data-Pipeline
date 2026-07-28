"""Pure-Python and Spark-column validation using the same rule vocabulary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from pydantic import ValidationError

from src.streaming.schema_registry import (
    ALLOWED_EVENT_TYPES,
    ALLOWED_REGIONS,
    SUPPORTED_PAYLOAD_VERSIONS,
    EventRecord,
)
from src.utils.time_utils import parse_timestamp, utc_now


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    record: dict[str, Any] | None = None
    error_type: str | None = None
    error_message: str | None = None
    failed_field: str | None = None


def validate_event(
    payload: str | bytes | dict[str, Any],
    *,
    now: datetime | None = None,
    future_tolerance_minutes: int = 5,
) -> ValidationResult:
    try:
        raw = json.loads(payload) if isinstance(payload, (str, bytes)) else dict(payload)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return ValidationResult(False, error_type="malformed_json", error_message=str(exc))

    try:
        event = EventRecord.model_validate(raw)
    except ValidationError as exc:
        error = exc.errors()[0]
        field = ".".join(str(part) for part in error.get("loc", [])) or None
        return ValidationResult(
            False,
            error_type="schema_validation",
            error_message=error.get("msg", "schema validation failed"),
            failed_field=field,
        )

    record = event.model_dump()
    if event.event_type not in ALLOWED_EVENT_TYPES:
        return _quality_error("event_type", f"unsupported event type: {event.event_type}")
    if event.region not in ALLOWED_REGIONS:
        return _quality_error("region", f"unsupported region: {event.region}")
    if event.payload_version not in SUPPORTED_PAYLOAD_VERSIONS:
        return _quality_error(
            "payload_version", f"unsupported payload version: {event.payload_version}"
        )
    if not event.user_id and not event.device_id:
        return _quality_error("user_id|device_id", "user_id or device_id is required")
    if not event.metadata:
        return _quality_error("metadata", "metadata must not be empty")
    if (
        event.event_value is not None
        and event.event_value < 0
        and event.event_type != "error_event"
    ):
        return _quality_error("event_value", "event_value cannot be negative for this event type")
    try:
        event_time = parse_timestamp(event.event_timestamp)
        parse_timestamp(event.ingestion_timestamp)
    except (ValueError, TypeError) as exc:
        return ValidationResult(
            False,
            error_type="malformed_timestamp",
            error_message=str(exc),
            failed_field="event_timestamp",
        )
    if event_time > (now or utc_now()) + timedelta(minutes=future_tolerance_minutes):
        return _quality_error("event_timestamp", "event timestamp is too far in the future")
    return ValidationResult(True, record=record)


def _quality_error(field: str, message: str) -> ValidationResult:
    return ValidationResult(
        False, error_type="quality_rule", error_message=message, failed_field=field
    )


def with_validation_columns(dataframe, future_tolerance_minutes: int = 5):
    """Attach one deterministic validation error to each parsed Spark row."""
    from pyspark.sql import functions as F

    allowed_types = list(ALLOWED_EVENT_TYPES)
    allowed_regions = list(ALLOWED_REGIONS)
    versions = list(SUPPORTED_PAYLOAD_VERSIONS)
    event_ts = F.to_timestamp("event_timestamp")
    ingestion_ts = F.to_timestamp("ingestion_timestamp")
    raw_event_value = F.get_json_object("raw_payload", "$.event_value")

    error_type = (
        F.when(F.col("_parsed").isNull(), F.lit("malformed_json"))
        .when(F.col("event_id").isNull() | (F.trim("event_id") == ""), F.lit("schema_validation"))
        .when(event_ts.isNull(), F.lit("malformed_timestamp"))
        .when(
            F.col("event_type").isNull() | (F.trim("event_type") == ""),
            F.lit("schema_validation"),
        )
        .when(~F.col("event_type").isin(allowed_types), F.lit("quality_rule"))
        .when(
            F.col("user_id").isNull() & F.col("device_id").isNull(),
            F.lit("quality_rule"),
        )
        .when(
            F.col("source_system").isNull() | (F.trim("source_system") == ""),
            F.lit("schema_validation"),
        )
        .when(F.col("region").isNull(), F.lit("schema_validation"))
        .when(~F.col("region").isin(allowed_regions), F.lit("quality_rule"))
        .when(F.col("payload_version").isNull(), F.lit("schema_validation"))
        .when(~F.col("payload_version").isin(versions), F.lit("quality_rule"))
        .when(
            F.col("metadata").isNull() | (F.size(F.col("metadata")) == 0),
            F.lit("quality_rule"),
        )
        .when(
            raw_event_value.isNotNull() & F.col("event_value").isNull(),
            F.lit("quality_rule"),
        )
        .when(
            (F.col("event_value") < 0) & (F.col("event_type") != "error_event"),
            F.lit("quality_rule"),
        )
        .when(
            event_ts
            > F.current_timestamp()
            + F.expr(f"INTERVAL {int(future_tolerance_minutes)} MINUTES"),
            F.lit("quality_rule"),
        )
        .when(ingestion_ts.isNull(), F.lit("malformed_timestamp"))
    )
    failed_field = (
        F.when(F.col("_parsed").isNull(), F.lit(None).cast("string"))
        .when(F.col("event_id").isNull() | (F.trim("event_id") == ""), F.lit("event_id"))
        .when(event_ts.isNull(), F.lit("event_timestamp"))
        .when(
            F.col("event_type").isNull() | (F.trim("event_type") == ""),
            F.lit("event_type"),
        )
        .when(~F.col("event_type").isin(allowed_types), F.lit("event_type"))
        .when(F.col("user_id").isNull() & F.col("device_id").isNull(), F.lit("user_id|device_id"))
        .when(
            F.col("source_system").isNull() | (F.trim("source_system") == ""),
            F.lit("source_system"),
        )
        .when(F.col("region").isNull(), F.lit("region"))
        .when(~F.col("region").isin(allowed_regions), F.lit("region"))
        .when(F.col("payload_version").isNull(), F.lit("payload_version"))
        .when(~F.col("payload_version").isin(versions), F.lit("payload_version"))
        .when(
            F.col("metadata").isNull() | (F.size(F.col("metadata")) == 0),
            F.lit("metadata"),
        )
        .when(
            raw_event_value.isNotNull() & F.col("event_value").isNull(),
            F.lit("event_value"),
        )
        .when(F.col("event_value") < 0, F.lit("event_value"))
        .when(event_ts > F.current_timestamp(), F.lit("event_timestamp"))
        .when(ingestion_ts.isNull(), F.lit("ingestion_timestamp"))
    )
    return (
        dataframe.withColumn("error_type", error_type)
        .withColumn("failed_field", failed_field)
        .withColumn(
            "error_message",
            F.when(
                error_type == "malformed_json",
                F.lit("payload is not valid JSON or does not match the event schema"),
            ).when(
                error_type.isNotNull(),
                F.concat(
                    F.lit("validation failed: "),
                    F.coalesce(failed_field, F.lit("unknown field")),
                ),
            ),
        )
        .withColumn("is_valid", error_type.isNull())
    )

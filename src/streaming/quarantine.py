"""Quarantine envelope construction."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.streaming.validation import ValidationResult
from src.utils.time_utils import utc_now_iso


@dataclass
class QuarantineRecord:
    original_payload: Any
    error_type: str
    error_message: str
    failed_field: str | None
    processing_timestamp: str
    replay_eligible: bool
    retry_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_quarantine_record(
    original_payload: Any, result: ValidationResult, retry_count: int = 0
) -> dict[str, Any]:
    if result.is_valid:
        raise ValueError("Cannot quarantine a valid record")
    return QuarantineRecord(
        original_payload=original_payload,
        error_type=result.error_type or "unknown_validation_error",
        error_message=result.error_message or "validation failed",
        failed_field=result.failed_field,
        processing_timestamp=utc_now_iso(),
        replay_eligible=result.error_type not in {"malformed_json"},
        retry_count=retry_count,
    ).to_dict()


def quarantine_dataframe(invalid_dataframe):
    from pyspark.sql import functions as F

    return invalid_dataframe.select(
        "event_id",
        F.col("raw_payload").alias("original_payload"),
        "error_type",
        "error_message",
        "failed_field",
        F.current_timestamp().cast("string").alias("processing_timestamp"),
        (~F.col("error_type").isin("malformed_json")).alias("replay_eligible"),
        F.lit(0).alias("retry_count"),
    )

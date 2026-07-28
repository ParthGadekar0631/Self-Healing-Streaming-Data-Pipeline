"""Classify operational failures and detect unhealthy pipeline conditions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from src.utils.time_utils import utc_now_iso


class FailureType(StrEnum):
    KAFKA = "kafka_failure"
    SPARK_QUERY = "spark_query_failure"
    CHECKPOINT = "checkpoint_failure"
    REDIS = "redis_failure"
    QUARANTINE_RATE = "excessive_quarantine_rate"
    OUTPUT_WRITE = "output_write_failure"
    UNKNOWN = "unknown_failure"


@dataclass
class Failure:
    failure_type: FailureType
    message: str
    component: str
    detected_at: str
    recoverable: bool
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FailureDetector:
    def __init__(self, quarantine_threshold: float = 0.10) -> None:
        self.quarantine_threshold = quarantine_threshold

    def classify(self, exception: BaseException, **metadata: Any) -> Failure:
        text = f"{exception.__class__.__name__}: {exception}".lower()
        if any(word in text for word in ("kafka", "broker", "topic", "timeout")):
            kind, component, recoverable = FailureType.KAFKA, "kafka", True
        elif any(word in text for word in ("redis", "connection refused")):
            kind, component, recoverable = FailureType.REDIS, "redis", True
        elif "checkpoint" in text or "offset log" in text:
            kind, component, recoverable = FailureType.CHECKPOINT, "checkpoint", True
        elif any(word in text for word in ("parquet", "filesystem", "permission denied", "write")):
            kind, component, recoverable = FailureType.OUTPUT_WRITE, "parquet_sink", True
        elif any(word in text for word in ("streamingquery", "spark", "py4j")):
            kind, component, recoverable = FailureType.SPARK_QUERY, "spark", True
        else:
            kind, component, recoverable = FailureType.UNKNOWN, "pipeline", False
        return Failure(kind, str(exception), component, utc_now_iso(), recoverable, metadata)

    def quarantine_rate_failure(self, invalid: int, total: int) -> Failure | None:
        rate = invalid / total if total else 0.0
        if rate <= self.quarantine_threshold:
            return None
        return Failure(
            FailureType.QUARANTINE_RATE,
            f"Quarantine rate {rate:.2%} exceeds {self.quarantine_threshold:.2%}",
            "validation",
            utc_now_iso(),
            True,
            {"invalid_records": invalid, "total_records": total, "quarantine_rate": rate},
        )

    @staticmethod
    def checkpoint_issue(path: Path) -> Failure | None:
        if not path.exists():
            return Failure(
                FailureType.CHECKPOINT,
                f"Checkpoint path does not exist: {path}",
                "checkpoint",
                utc_now_iso(),
                True,
                {"checkpoint": str(path), "reason": "missing"},
            )
        marker = path / "_CORRUPT"
        if marker.exists():
            return Failure(
                FailureType.CHECKPOINT,
                f"Checkpoint corruption marker found: {marker}",
                "checkpoint",
                utc_now_iso(),
                True,
                {"checkpoint": str(path), "reason": "corrupt"},
            )
        return None

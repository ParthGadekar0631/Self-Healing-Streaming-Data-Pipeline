"""Collect and persist bounded incident evidence and reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import logging

from src.utils.file_utils import atomic_write_json, read_json
from src.utils.time_utils import utc_now_iso

logger = logging.getLogger(__name__)

class IncidentCollector:
    def __init__(self, metadata_dir: Path, parquet_dir: Path | None = None) -> None:
        self.metadata_dir = metadata_dir
        self.incidents_path = metadata_dir / "incidents.json"
        self.parquet_dir = parquet_dir

    def from_failure(self, failure: dict[str, Any]) -> dict[str, Any]:
        metadata = failure.get("metadata", {})
        return {
            "exception_message": failure.get("message"),
            "failure_type": failure.get("failure_type"),
            "failed_topic": metadata.get("topic"),
            "failed_batch_id": metadata.get("batch_id"),
            "failed_records": metadata.get("failed_records", 0),
            "quarantine_rate": metadata.get("quarantine_rate", 0),
            "retry_attempts": metadata.get("retry_attempts", 0),
            "latest_checkpoint": metadata.get("checkpoint"),
            "redis_status": metadata.get("redis_status", "unknown"),
            "kafka_status": metadata.get("kafka_status", "unknown"),
            "collected_at": utc_now_iso(),
        }

    def persist_report(self, report: dict[str, Any]) -> None:
        incidents = self.list_reports()
        incidents.append(report)
        atomic_write_json(self.incidents_path, incidents[-1_000:])
        self._persist_parquet(report)

    def _persist_parquet(self, report: dict[str, Any]) -> None:
        if self.parquet_dir is None:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            self.parquet_dir.mkdir(parents=True, exist_ok=True)
            flat = {
                key: (
                    json.dumps(value, default=str)
                    if isinstance(value, (dict, list))
                    else value
                )
                for key, value in report.items()
            }
            table = pa.Table.from_pylist([flat])
            incident_id = str(report.get("incident_id", "unknown"))
            pq.write_table(table, self.parquet_dir / f"{incident_id}.parquet")
        except ImportError:
            logger.info("pyarrow unavailable; incident remains persisted in JSON")
        except Exception as exc:
            # Incident reporting must never take down recovery because an auxiliary
            # analytics copy could not be written.
            logger.warning("Unable to persist incident Parquet copy: %s", exc)

    def list_reports(self) -> list[dict[str, Any]]:
        reports = read_json(self.incidents_path, []) or []
        return reports if isinstance(reports, list) else []

    def latest(self) -> dict[str, Any] | None:
        reports = self.list_reports()
        return reports[-1] if reports else None

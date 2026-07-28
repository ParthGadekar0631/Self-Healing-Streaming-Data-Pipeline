"""Operational services behind the API routes."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from src.config import Settings
from src.incident_ai.incident_collector import IncidentCollector
from src.incident_ai.incident_summarizer import IncidentSummarizer
from src.recovery.idempotency import IdempotencyStore
from src.recovery.replay_manager import ReplayManager
from src.recovery.retry_manager import RetryManager
from src.utils.file_utils import atomic_write_json, read_json
from src.utils.time_utils import utc_now_iso


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode(),
            acks="all",
        )

    def publish(self, topic: str, value: dict[str, Any]) -> None:
        self.producer.send(topic, value=value).get(timeout=15)

    def close(self) -> None:
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)


class PipelineService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.collector = IncidentCollector(
            settings.metadata_dir,
            settings.parquet_output_dir / "incident_logs",
        )
        self.summarizer = IncidentSummarizer(settings)

    def status(self) -> dict[str, Any]:
        return read_json(
            self.settings.metadata_dir / "pipeline_status.json",
            {"status": "not_started"},
        )

    def metrics(self) -> dict[str, Any]:
        return read_json(self.settings.metadata_dir / "pipeline_metrics.json", {})

    def incidents(self) -> list[dict[str, Any]]:
        return self.collector.list_reports()

    def summarize(self, context: dict[str, Any]) -> dict[str, Any]:
        report = self.summarizer.summarize(context)
        self.collector.persist_report(report)
        return report

    def quarantine_records(self, limit: int = 100) -> list[dict[str, Any]]:
        records = read_json(self.settings.metadata_dir / "quarantine_records.json", []) or []
        return records[-limit:]

    def quarantine_stats(self) -> dict[str, Any]:
        records = self.quarantine_records(10_000)
        by_error: dict[str, int] = {}
        eligible = 0
        for record in records:
            name = str(record.get("error_type", "unknown"))
            by_error[name] = by_error.get(name, 0) + 1
            eligible += int(bool(record.get("replay_eligible")))
        return {
            "indexed_records": len(records),
            "replay_eligible": eligible,
            "by_error_type": by_error,
            "pipeline_quarantine_rate": self.metrics().get("quarantine_rate", 0),
        }

    def replay(self, limit: int, dry_run: bool) -> dict[str, Any]:
        records = self.quarantine_records(limit)
        manager = ReplayManager(
            self.settings.kafka_replay_topic,
            self.settings.kafka_raw_topic,
            self.settings.kafka_dead_letter_topic,
        )
        decisions = [manager.prepare(record) for record in records]
        if dry_run:
            return {
                "dry_run": True,
                "records": len(records),
                "destinations": [decision.destination for decision in decisions],
            }
        publisher = KafkaPublisher(self.settings.kafka_bootstrap_servers)
        try:
            result = manager.replay(records, publisher)
        finally:
            publisher.close()
        atomic_write_json(
            self.settings.metadata_dir / "last_replay.json",
            {"completed_at": utc_now_iso(), **result},
        )
        return result

    def retry_health_check(self, attempts: int | None = None) -> dict[str, Any]:
        store = IdempotencyStore(self.settings)
        retry = RetryManager(
            max_attempts=attempts or self.settings.max_retry_attempts,
            base_delay_seconds=self.settings.retry_base_delay_seconds,
            max_delay_seconds=self.settings.retry_max_delay_seconds,
        )

        def verify() -> bool:
            if not store.healthy():
                raise ConnectionError("Redis health check failed")
            return True

        retry.call(verify)
        return {"status": "healthy", "component": "redis", "retry_history": retry.serialized_history()}

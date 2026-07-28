"""Offline, deterministic incident summary provider."""

from __future__ import annotations

from typing import Any

from src.incident_ai.recommendation_engine import recommendations


class MockAIProvider:
    name = "mock"

    def summarize(self, context: dict[str, Any]) -> dict[str, Any]:
        exception = str(context.get("exception_message") or "Pipeline health threshold exceeded")
        text = exception.lower()
        quarantine_rate = float(context.get("quarantine_rate") or 0)
        attempts = int(context.get("retry_attempts") or 0)

        if context.get("redis_status") in {"down", "fallback"} or "redis" in text:
            component, cause = "Redis", "Redis is unavailable or timing out"
        elif context.get("kafka_status") == "down" or any(
            token in text for token in ("kafka", "broker", "topic")
        ):
            component, cause = "Kafka", "Kafka broker or topic connectivity is degraded"
        elif "checkpoint" in text:
            component, cause = "Spark checkpoint", "Checkpoint state is missing or inconsistent"
        elif quarantine_rate > 0.10:
            component, cause = "Validation", "Incoming payload quality or schema compatibility regressed"
        elif any(token in text for token in ("parquet", "write", "filesystem")):
            component, cause = "Parquet sink", "Output storage rejected or interrupted a write"
        else:
            component, cause = "Streaming pipeline", "The available evidence is insufficient for a precise cause"

        if attempts >= 5 or quarantine_rate >= 0.5:
            severity = "critical"
        elif attempts >= 3 or quarantine_rate > 0.10:
            severity = "high"
        elif exception:
            severity = "medium"
        else:
            severity = "low"

        return {
            "incident_title": f"{component} pipeline incident",
            "severity": severity,
            "summary": (
                f"The streaming pipeline reported: {exception}. "
                f"Retry attempts: {attempts}; quarantine rate: {quarantine_rate:.2%}."
            ),
            "likely_root_cause": cause,
            "affected_components": [component, "PySpark Structured Streaming"],
            "recommended_recovery_steps": recommendations(context),
            "prevention_notes": [
                "Alert on dependency health and quarantine-rate changes before the failure threshold.",
                "Test checkpoint recovery and replay paths regularly with controlled fault injection.",
            ],
            "provider": self.name,
        }

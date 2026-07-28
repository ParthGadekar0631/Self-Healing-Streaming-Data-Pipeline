"""Deterministic recovery recommendations keyed by failure evidence."""

from __future__ import annotations

from typing import Any


def recommendations(context: dict[str, Any]) -> list[str]:
    text = " ".join(str(value) for value in context.values()).lower()
    steps: list[str] = []
    if "redis" in text or context.get("redis_status") in {"down", "fallback"}:
        steps.extend(
            [
                "Keep the SQLite degraded-mode idempotency store enabled while Redis is restored.",
                "Verify Redis connectivity and memory pressure, then reconcile fallback event IDs.",
            ]
        )
    if "kafka" in text or context.get("kafka_status") == "down":
        steps.extend(
            [
                "Verify broker health, topic leaders, authentication, and network reachability.",
                "Restart the query from its last committed Kafka offsets after the broker stabilizes.",
            ]
        )
    if "checkpoint" in text:
        steps.append(
            "Validate checkpoint commit/offset logs; archive only the affected query checkpoint if corrupt."
        )
    if float(context.get("quarantine_rate") or 0) > 0.10:
        steps.append(
            "Compare quarantined payload versions with the active schema and pause incompatible producers."
        )
    if "parquet" in text or "write" in text:
        steps.append("Check output permissions, capacity, and partial files before retrying the write.")
    if not steps:
        steps.append("Inspect the failing batch logs and retry the recoverable operation from checkpoint.")
    return steps

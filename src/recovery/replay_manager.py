"""Quarantine record replay and dead-letter routing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from src.streaming.validation import validate_event
from src.utils.time_utils import utc_now_iso


class Publisher(Protocol):
    def publish(self, topic: str, value: dict[str, Any]) -> None: ...


@dataclass(frozen=True)
class ReplayDecision:
    destination: str
    payload: dict[str, Any]
    reason: str


class ReplayManager:
    def __init__(
        self,
        replay_topic: str = "events.replay",
        raw_topic: str = "events.raw",
        dead_letter_topic: str = "events.dead_letter",
        max_retry_count: int = 3,
    ) -> None:
        self.replay_topic = replay_topic
        self.raw_topic = raw_topic
        self.dead_letter_topic = dead_letter_topic
        self.max_retry_count = max_retry_count

    def prepare(self, record: dict[str, Any]) -> ReplayDecision:
        retry_count = int(record.get("retry_count", 0))
        if not record.get("replay_eligible", False):
            return ReplayDecision(self.dead_letter_topic, record, "record is not replay eligible")
        if retry_count >= self.max_retry_count:
            return ReplayDecision(self.dead_letter_topic, record, "maximum replay attempts exceeded")

        original = record.get("original_payload")
        try:
            payload = json.loads(original) if isinstance(original, str) else dict(original)
        except (TypeError, ValueError, json.JSONDecodeError):
            return ReplayDecision(self.dead_letter_topic, record, "original payload is malformed")

        validation = validate_event(payload)
        if not validation.is_valid:
            failed = dict(record)
            failed["retry_count"] = retry_count + 1
            failed["last_replay_error"] = validation.error_message
            failed["last_replay_at"] = utc_now_iso()
            destination = (
                self.dead_letter_topic
                if failed["retry_count"] >= self.max_retry_count
                else self.replay_topic
            )
            return ReplayDecision(destination, failed, "payload still fails validation")

        replay_payload = dict(validation.record or payload)
        replay_payload["_replay"] = {
            "replayed_at": utc_now_iso(),
            "retry_count": retry_count + 1,
            "source": "quarantine",
        }
        return ReplayDecision(self.replay_topic, replay_payload, "eligible and valid")

    def replay(self, records: list[dict[str, Any]], publisher: Publisher) -> dict[str, int]:
        counts = {"replayed": 0, "dead_lettered": 0, "deferred": 0}
        for record in records:
            decision = self.prepare(record)
            publisher.publish(decision.destination, decision.payload)
            if decision.destination == self.dead_letter_topic:
                counts["dead_lettered"] += 1
            elif decision.reason == "eligible and valid":
                publisher.publish(self.raw_topic, decision.payload)
                counts["replayed"] += 1
            else:
                counts["deferred"] += 1
        return counts

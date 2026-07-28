"""Generate realistic valid and deliberately invalid event payloads."""

from __future__ import annotations

import random
import uuid
from copy import deepcopy
from datetime import timedelta
from typing import Any, Literal

from src.utils.time_utils import utc_now, utc_now_iso


EVENT_TYPES = ("page_view", "click", "purchase", "sensor_reading", "error_event", "heartbeat")
REGIONS = ("us-east", "us-west", "eu-west", "ap-south")
SOURCES = ("web", "mobile", "iot-gateway", "partner-api")
InvalidKind = Literal[
    "missing_event_id",
    "missing_event_timestamp",
    "invalid_event_type",
    "negative_event_value",
    "malformed_timestamp",
    "duplicate_event_id",
    "missing_metadata",
    "unsupported_payload_version",
]


class EventGenerator:
    def __init__(self, seed: int | None = None) -> None:
        self.random = random.Random(seed)
        self._last_event: dict[str, Any] | None = None

    def valid_event(self) -> dict[str, Any]:
        event_type = self.random.choice(EVENT_TYPES)
        now = utc_now()
        has_user = self.random.random() > 0.15
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": f"usr-{self.random.randint(1, 50_000)}" if has_user else None,
            "device_id": f"dev-{self.random.randint(1, 10_000)}",
            "event_type": event_type,
            "event_timestamp": (now - timedelta(seconds=self.random.randint(0, 120))).isoformat(),
            "ingestion_timestamp": now.isoformat(),
            "source_system": self.random.choice(SOURCES),
            "region": self.random.choice(REGIONS),
            "session_id": f"ses-{uuid.uuid4().hex[:12]}",
            "event_value": round(self.random.uniform(0, 500), 2),
            "event_status": "error" if event_type == "error_event" else "accepted",
            "payload_version": self.random.choice(("1.0", "1.1")),
            "metadata": {
                "generator": "synthetic",
                "trace_id": uuid.uuid4().hex,
                "generated_at": utc_now_iso(),
            },
        }
        self._last_event = deepcopy(event)
        return event

    def invalid_event(self, kind: InvalidKind | None = None) -> dict[str, Any]:
        kinds: tuple[InvalidKind, ...] = (
            "missing_event_id",
            "missing_event_timestamp",
            "invalid_event_type",
            "negative_event_value",
            "malformed_timestamp",
            "duplicate_event_id",
            "missing_metadata",
            "unsupported_payload_version",
        )
        selected = kind or self.random.choice(kinds)
        if selected == "duplicate_event_id" and self._last_event:
            event = deepcopy(self._last_event)
            event["ingestion_timestamp"] = utc_now_iso()
            return event

        event = self.valid_event()
        if selected == "missing_event_id":
            event.pop("event_id")
        elif selected == "missing_event_timestamp":
            event.pop("event_timestamp")
        elif selected == "invalid_event_type":
            event["event_type"] = "video_autoplayed"
        elif selected == "negative_event_value":
            event["event_type"] = "purchase"
            event["event_value"] = -99.0
        elif selected == "malformed_timestamp":
            event["event_timestamp"] = "tomorrow-ish"
        elif selected == "missing_metadata":
            event["metadata"] = {}
        elif selected == "unsupported_payload_version":
            event["payload_version"] = "99.0"
        return event

    def generate(self, count: int, invalid_rate: float = 0.1) -> list[dict[str, Any]]:
        if count < 0 or not 0 <= invalid_rate <= 1:
            raise ValueError("count must be non-negative and invalid_rate must be between 0 and 1")
        return [
            self.invalid_event() if self.random.random() < invalid_rate else self.valid_event()
            for _ in range(count)
        ]

"""Redis-backed event deduplication with an in-memory test implementation."""

from __future__ import annotations

from threading import Lock
from typing import Protocol


class DedupStore(Protocol):
    def claim(self, event_id: str, status: str = "received") -> bool: ...
    def set_status(self, event_id: str, status: str) -> None: ...
    def get_status(self, event_id: str) -> str | None: ...


class InMemoryDeduplicator:
    def __init__(self) -> None:
        self._records: dict[str, str] = {}
        self._lock = Lock()

    def claim(self, event_id: str, status: str = "received") -> bool:
        with self._lock:
            if event_id in self._records:
                return False
            self._records[event_id] = status
            return True

    def set_status(self, event_id: str, status: str) -> None:
        with self._lock:
            self._records[event_id] = status

    def get_status(self, event_id: str) -> str | None:
        return self._records.get(event_id)


def watermark_deduplicate(dataframe, watermark: str = "10 minutes"):
    return dataframe.withWatermark("event_timestamp", watermark).dropDuplicates(["event_id"])

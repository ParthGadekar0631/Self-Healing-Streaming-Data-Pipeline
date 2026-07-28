"""Redis idempotency state with durable local SQLite degraded mode."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

import redis

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)
VALID_STATUSES = {
    "received",
    "validated",
    "transformed",
    "written",
    "quarantined",
    "replayed",
}


class SQLiteFallbackStore:
    def __init__(self, path: Path, ttl_seconds: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS events "
                "(event_id TEXT PRIMARY KEY, status TEXT NOT NULL, expires_at REAL NOT NULL)"
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=5)

    def claim(self, event_id: str, status: str) -> bool:
        now = time.time()
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM events WHERE expires_at <= ?", (now,))
            cursor = connection.execute(
                "INSERT OR IGNORE INTO events(event_id, status, expires_at) VALUES (?, ?, ?)",
                (event_id, status, now + self.ttl_seconds),
            )
            return cursor.rowcount == 1

    def set_status(self, event_id: str, status: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                "INSERT INTO events(event_id, status, expires_at) VALUES (?, ?, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET status=excluded.status, "
                "expires_at=excluded.expires_at",
                (event_id, status, time.time() + self.ttl_seconds),
            )

    def get_status(self, event_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status, expires_at FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        if not row or row[1] <= time.time():
            return None
        return str(row[0])


class IdempotencyStore:
    """Atomic Redis claims; transparently degrades to SQLite during outages."""

    prefix = "pipeline:event:"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: redis.Redis | None = None,
        fallback: SQLiteFallbackStore | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or redis.Redis(
            host=self.settings.redis_host,
            port=self.settings.redis_port,
            db=self.settings.redis_db,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        self.fallback = fallback or SQLiteFallbackStore(
            self.settings.metadata_dir / "idempotency.sqlite3",
            self.settings.redis_ttl_seconds,
        )
        self.using_fallback = False

    def _key(self, event_id: str) -> str:
        if not event_id:
            raise ValueError("event_id cannot be empty")
        return f"{self.prefix}{event_id}"

    def claim(self, event_id: str, status: str = "received") -> bool:
        self._validate_status(status)
        try:
            claimed = bool(
                self.client.set(
                    self._key(event_id),
                    status,
                    ex=self.settings.redis_ttl_seconds,
                    nx=True,
                )
            )
            if claimed:
                self.fallback.set_status(event_id, status)
            self.using_fallback = False
            return claimed
        except redis.RedisError as exc:
            self.using_fallback = True
            logger.warning("Redis unavailable; using SQLite idempotency fallback: %s", exc)
            return self.fallback.claim(event_id, status)

    def set_status(self, event_id: str, status: str) -> None:
        self._validate_status(status)
        # Maintain a local shadow even while Redis is healthy so a later outage
        # cannot make already-written events appear unseen.
        self.fallback.set_status(event_id, status)
        try:
            self.client.set(
                self._key(event_id), status, ex=self.settings.redis_ttl_seconds
            )
            self.using_fallback = False
        except redis.RedisError as exc:
            self.using_fallback = True
            logger.warning("Redis unavailable; writing local status: %s", exc)
            self.fallback.set_status(event_id, status)

    def get_status(self, event_id: str) -> str | None:
        try:
            value = self.client.get(self._key(event_id))
            self.using_fallback = False
            return str(value) if value is not None else self.fallback.get_status(event_id)
        except redis.RedisError:
            self.using_fallback = True
            return self.fallback.get_status(event_id)

    def healthy(self) -> bool:
        try:
            return bool(self.client.ping())
        except redis.RedisError:
            return False

    @staticmethod
    def _validate_status(status: str) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(f"Unsupported processing status: {status}")

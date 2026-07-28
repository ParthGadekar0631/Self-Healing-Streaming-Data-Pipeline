"""Bounded exponential retry policy with observable history."""

from __future__ import annotations

import logging
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from src.config import Settings

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class RetryEvent:
    operation: str
    attempt: int
    exception: str
    timestamp: str


class RetryManager:
    def __init__(
        self,
        max_attempts: int = 5,
        base_delay_seconds: float = 2,
        max_delay_seconds: float = 30,
        *,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.sleep = sleep
        self.history: list[RetryEvent] = []

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetryManager":
        return cls(
            settings.max_retry_attempts,
            settings.retry_base_delay_seconds,
            settings.retry_max_delay_seconds,
        )

    def compute_delay(self, failed_attempt: int, jitter: float = 0) -> float:
        base = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, failed_attempt - 1)),
        )
        return min(self.max_delay_seconds, base + random.uniform(0, jitter))

    def call(
        self,
        operation: Callable[..., T],
        *args: Any,
        retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
        **kwargs: Any,
    ) -> T:
        operation_name = getattr(operation, "__name__", operation.__class__.__name__)

        def before_sleep(state) -> None:
            exception = state.outcome.exception() if state.outcome else None
            event = RetryEvent(
                operation=operation_name,
                attempt=state.attempt_number,
                exception=str(exception),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            self.history.append(event)
            logger.warning(
                "Retrying %s after attempt %s: %s",
                operation_name,
                state.attempt_number,
                exception,
                extra={"action": "retry"},
            )

        retrying = Retrying(
            stop=stop_after_attempt(self.max_attempts),
            wait=wait_exponential_jitter(
                initial=self.base_delay_seconds,
                max=self.max_delay_seconds,
                jitter=0,
            ),
            retry=retry_if_exception_type(retry_exceptions),
            before_sleep=before_sleep,
            reraise=True,
            sleep=self.sleep if self.sleep is not None else __import__("time").sleep,
        )
        return retrying(operation, *args, **kwargs)

    def serialized_history(self) -> list[dict[str, Any]]:
        return [asdict(item) for item in self.history]

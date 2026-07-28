import pytest

from src.recovery.retry_manager import RetryManager


def test_retry_manager_applies_exponential_backoff():
    sleeps = []
    retry = RetryManager(
        max_attempts=3,
        base_delay_seconds=1,
        max_delay_seconds=10,
        sleep=sleeps.append,
    )
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectionError("temporary")
        return "ok"

    assert retry.call(flaky) == "ok"
    assert attempts["count"] == 3
    assert sleeps == [1.0, 2.0]
    assert len(retry.history) == 2


def test_compute_delay_is_capped():
    retry = RetryManager(base_delay_seconds=2, max_delay_seconds=5)
    assert retry.compute_delay(1) == 2
    assert retry.compute_delay(10) == 5

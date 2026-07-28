"""CLI Kafka producer with retries and JSON serialization."""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterable
from typing import Any

from kafka import KafkaProducer

from src.config import Settings, get_settings
from src.producer.event_generator import EventGenerator
from src.recovery.retry_manager import RetryManager
from src.utils.logger import configure_logging

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.retry = RetryManager.from_settings(self.settings)
        self._producer: KafkaProducer | None = None

    def connect(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = self.retry.call(
                KafkaProducer,
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8") if key else None,
                acks="all",
                retries=3,
                enable_idempotence=True,
            )
        return self._producer

    def send(self, event: dict[str, Any], topic: str | None = None) -> None:
        target = topic or self.settings.kafka_raw_topic
        event_id = str(event.get("event_id", "missing"))
        future = self.connect().send(target, key=event_id, value=event)
        future.get(timeout=20)
        logger.info("Published event", extra={"event_id": event_id, "topic": target})

    def send_many(self, events: Iterable[dict[str, Any]], interval_seconds: float = 0) -> int:
        count = 0
        for event in events:
            self.retry.call(self.send, event)
            count += 1
            if interval_seconds:
                time.sleep(interval_seconds)
        if self._producer:
            self._producer.flush(timeout=30)
        return count

    def close(self) -> None:
        if self._producer:
            self._producer.close(timeout=10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish synthetic events to Kafka")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--invalid-rate", type=float, default=0.1)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--topic")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    generator = EventGenerator(seed=args.seed)
    producer = EventProducer(settings)
    try:
        sent = producer.send_many(
            generator.generate(args.count, args.invalid_rate), args.interval
        )
        logger.info("Production complete: %s events", sent)
    finally:
        producer.close()


if __name__ == "__main__":
    main()

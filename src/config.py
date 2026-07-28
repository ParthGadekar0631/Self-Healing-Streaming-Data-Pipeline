"""Typed application configuration loaded from environment and YAML files."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Runtime settings. Environment variables override the documented defaults."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_raw_topic: str = "events.raw"
    kafka_validated_topic: str = "events.validated"
    kafka_transformed_topic: str = "events.transformed"
    kafka_aggregated_topic: str = "events.aggregated"
    kafka_quarantine_topic: str = "events.quarantine"
    kafka_dead_letter_topic: str = "events.dead_letter"
    kafka_replay_topic: str = "events.replay"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_ttl_seconds: int = 86_400
    redis_db: int = 0

    spark_app_name: str = "SelfHealingStreamingPipeline"
    spark_master: str = "local[*]"
    checkpoint_dir: Path = PROJECT_ROOT / "data" / "checkpoints"
    parquet_output_dir: Path = PROJECT_ROOT / "data" / "parquet"
    metadata_dir: Path = PROJECT_ROOT / "data" / "metadata"

    max_retry_attempts: int = Field(default=5, ge=1, le=20)
    retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    retry_max_delay_seconds: float = Field(default=30.0, gt=0)
    quarantine_rate_alert_threshold: float = Field(default=0.10, ge=0, le=1)
    dedup_watermark: str = "10 minutes"
    aggregation_window: str = "5 minutes"
    future_timestamp_tolerance_minutes: int = 5

    ai_provider: str = "mock"
    openai_api_key: str | None = None
    gemini_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"

    incident_api_host: str = "0.0.0.0"
    incident_api_port: int = 8000
    log_level: str = "INFO"

    @property
    def kafka_options(self) -> dict[str, str]:
        return {"kafka.bootstrap.servers": self.kafka_bootstrap_servers}

    def ensure_directories(self) -> None:
        for directory in (
            self.checkpoint_dir,
            self.parquet_output_dir / "clean_events",
            self.parquet_output_dir / "aggregates",
            self.parquet_output_dir / "incident_logs",
            self.metadata_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_directories()
    return settings


def load_yaml(name: str) -> dict[str, Any]:
    path = PROJECT_ROOT / "configs" / name
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return loaded

"""API request and response contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IncidentInput(BaseModel):
    exception_message: str = "Pipeline health threshold exceeded"
    failed_topic: str | None = None
    failed_batch_id: int | None = None
    failed_records: int = Field(default=0, ge=0)
    quarantine_rate: float = Field(default=0, ge=0, le=1)
    retry_attempts: int = Field(default=0, ge=0)
    latest_checkpoint: str | None = None
    redis_status: str = "unknown"
    kafka_status: str = "unknown"


class ReplayRequest(BaseModel):
    limit: int = Field(default=100, ge=1, le=10_000)
    dry_run: bool = False


class RetryRequest(BaseModel):
    operation: str = "dependency_health_check"
    max_attempts: int | None = Field(default=None, ge=1, le=10)


class OperationResult(BaseModel):
    status: str
    details: dict[str, Any] = Field(default_factory=dict)

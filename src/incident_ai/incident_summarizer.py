"""Provider adapter with mock-default and safe remote fallback."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Protocol

from src.config import Settings, get_settings
from src.incident_ai.mock_ai_provider import MockAIProvider
from src.utils.time_utils import utc_now_iso

logger = logging.getLogger(__name__)


class IncidentProvider(Protocol):
    name: str
    def summarize(self, context: dict[str, Any]) -> dict[str, Any]: ...


class OpenAIIncidentProvider:
    name = "openai"

    def __init__(self, api_key: str, model: str) -> None:
        from openai import OpenAI

        self.client = OpenAI(api_key=api_key)
        self.model = model

    def summarize(self, context: dict[str, Any]) -> dict[str, Any]:
        response = self.client.responses.create(
            model=self.model,
            instructions=(
                "You are an SRE incident analyst. Return only JSON with keys "
                "incident_title, severity, summary, likely_root_cause, affected_components, "
                "recommended_recovery_steps, prevention_notes. Do not invent unavailable facts."
            ),
            input=json.dumps(context, default=str),
        )
        result = json.loads(response.output_text)
        result["provider"] = self.name
        return result


class IncidentSummarizer:
    def __init__(
        self,
        settings: Settings | None = None,
        provider: IncidentProvider | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.fallback = MockAIProvider()
        if provider:
            self.provider = provider
        elif self.settings.ai_provider.lower() == "openai" and self.settings.openai_api_key:
            self.provider = OpenAIIncidentProvider(
                self.settings.openai_api_key, self.settings.openai_model
            )
        else:
            self.provider = self.fallback

    def summarize(self, context: dict[str, Any]) -> dict[str, Any]:
        try:
            report = self.provider.summarize(context)
            self._validate(report)
        except Exception as exc:
            logger.warning("Incident provider failed; using mock fallback: %s", exc)
            report = self.fallback.summarize(context)
            report["provider_fallback_reason"] = str(exc)
        report["incident_id"] = report.get("incident_id", f"inc-{uuid.uuid4().hex[:12]}")
        report["created_at"] = report.get("created_at", utc_now_iso())
        report["evidence"] = context
        return report

    @staticmethod
    def _validate(report: dict[str, Any]) -> None:
        required = {
            "incident_title",
            "severity",
            "summary",
            "likely_root_cause",
            "affected_components",
            "recommended_recovery_steps",
            "prevention_notes",
        }
        missing = required - set(report)
        if missing:
            raise ValueError(f"Incident provider omitted fields: {sorted(missing)}")

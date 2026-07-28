from src.config import Settings
from src.incident_ai.incident_summarizer import IncidentSummarizer


def test_incident_summarizer_returns_structured_report(tmp_path):
    settings = Settings(
        ai_provider="mock",
        metadata_dir=tmp_path,
        checkpoint_dir=tmp_path / "checkpoints",
        parquet_output_dir=tmp_path / "parquet",
    )
    report = IncidentSummarizer(settings).summarize(
        {
            "exception_message": "Redis connection refused",
            "redis_status": "down",
            "retry_attempts": 3,
            "quarantine_rate": 0.02,
        }
    )
    assert report["severity"] == "high"
    assert "Redis" in report["affected_components"]
    assert report["recommended_recovery_steps"]
    assert report["incident_id"].startswith("inc-")

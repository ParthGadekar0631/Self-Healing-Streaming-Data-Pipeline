from fastapi.testclient import TestClient

from src.api.main import create_app
from src.config import Settings


def test_health_and_mock_incident_endpoints(tmp_path):
    settings = Settings(
        ai_provider="mock",
        metadata_dir=tmp_path / "metadata",
        checkpoint_dir=tmp_path / "checkpoints",
        parquet_output_dir=tmp_path / "parquet",
    )
    settings.ensure_directories()
    client = TestClient(create_app(settings))
    assert client.get("/health").status_code == 200
    response = client.post(
        "/incidents/summarize",
        json={"exception_message": "Kafka broker unavailable", "kafka_status": "down"},
    )
    assert response.status_code == 200
    assert response.json()["provider"] == "mock"

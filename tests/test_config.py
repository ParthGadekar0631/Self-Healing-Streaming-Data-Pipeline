from src.config import Settings, load_yaml


def test_pipeline_config_loads_correctly(tmp_path):
    settings = Settings(
        metadata_dir=tmp_path / "metadata",
        checkpoint_dir=tmp_path / "checkpoints",
        parquet_output_dir=tmp_path / "parquet",
    )
    settings.ensure_directories()
    config = load_yaml("pipeline_config.yaml")
    assert config["pipeline"]["aggregation_window"] == "5 minutes"
    assert settings.kafka_raw_topic == "events.raw"
    assert settings.metadata_dir.exists()

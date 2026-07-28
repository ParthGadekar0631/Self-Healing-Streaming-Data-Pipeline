import json

from src.producer.event_generator import EventGenerator
from src.streaming.validation import validate_event


def test_validation_accepts_valid_record():
    result = validate_event(EventGenerator(seed=1).valid_event())
    assert result.is_valid
    assert result.record is not None


def test_validation_rejects_malformed_json():
    result = validate_event('{"event_id":')
    assert not result.is_valid
    assert result.error_type == "malformed_json"


def test_validation_rejects_quality_violation():
    payload = EventGenerator(seed=2).valid_event()
    payload["event_type"] = "unknown"
    result = validate_event(json.dumps(payload))
    assert not result.is_valid
    assert result.failed_field == "event_type"

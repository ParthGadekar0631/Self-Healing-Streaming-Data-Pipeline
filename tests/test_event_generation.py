from src.producer.event_generator import EVENT_TYPES, EventGenerator


def test_event_generator_creates_valid_events():
    event = EventGenerator(seed=7).valid_event()
    assert event["event_id"]
    assert event["event_type"] in EVENT_TYPES
    assert event["metadata"]["generator"] == "synthetic"


def test_event_generator_creates_invalid_events():
    generator = EventGenerator(seed=7)
    assert "event_id" not in generator.invalid_event("missing_event_id")
    assert generator.invalid_event("invalid_event_type")["event_type"] not in EVENT_TYPES
    assert generator.invalid_event("unsupported_payload_version")["payload_version"] == "99.0"


def test_generate_rejects_invalid_parameters():
    generator = EventGenerator()
    try:
        generator.generate(-1, 0.1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative count should fail")

from src.producer.event_generator import EventGenerator
from src.streaming.transformations import transform_event


def test_transformation_creates_expected_fields():
    event = EventGenerator(seed=3).valid_event()
    transformed = transform_event(event)
    assert transformed["event_date"]
    assert transformed["processed_at"]
    assert len(transformed["processing_key"]) == 64
    assert isinstance(transformed["event_value"], float)

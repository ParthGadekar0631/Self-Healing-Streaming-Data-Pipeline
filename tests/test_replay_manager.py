from src.producer.event_generator import EventGenerator
from src.recovery.replay_manager import ReplayManager


class FakePublisher:
    def __init__(self):
        self.messages = []

    def publish(self, topic, value):
        self.messages.append((topic, value))


def test_replay_manager_prepares_valid_quarantine_record():
    record = {
        "original_payload": EventGenerator(seed=4).valid_event(),
        "replay_eligible": True,
        "retry_count": 0,
    }
    manager = ReplayManager()
    decision = manager.prepare(record)
    assert decision.destination == "events.replay"
    assert decision.payload["_replay"]["source"] == "quarantine"


def test_replay_manager_routes_exhausted_record_to_dead_letter():
    manager = ReplayManager(max_retry_count=3)
    decision = manager.prepare(
        {"original_payload": {}, "replay_eligible": True, "retry_count": 3}
    )
    assert decision.destination == "events.dead_letter"


def test_replay_publishes_audit_and_raw_messages():
    publisher = FakePublisher()
    record = {
        "original_payload": EventGenerator(seed=5).valid_event(),
        "replay_eligible": True,
        "retry_count": 0,
    }
    result = ReplayManager().replay([record], publisher)
    assert result["replayed"] == 1
    assert [topic for topic, _ in publisher.messages] == ["events.replay", "events.raw"]

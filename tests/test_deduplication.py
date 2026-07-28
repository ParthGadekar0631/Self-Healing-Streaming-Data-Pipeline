from src.streaming.deduplication import InMemoryDeduplicator


def test_deduplication_skips_duplicate_event_id():
    store = InMemoryDeduplicator()
    assert store.claim("evt-1")
    assert not store.claim("evt-1")
    store.set_status("evt-1", "written")
    assert store.get_status("evt-1") == "written"

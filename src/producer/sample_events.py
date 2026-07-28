"""Human-readable sample records for demos and documentation."""

VALID_SAMPLE = {
    "event_id": "evt-demo-001",
    "user_id": "usr-42",
    "device_id": "dev-7",
    "event_type": "purchase",
    "event_timestamp": "2026-01-15T12:00:00Z",
    "ingestion_timestamp": "2026-01-15T12:00:01Z",
    "source_system": "web",
    "region": "us-east",
    "session_id": "session-12",
    "event_value": 49.99,
    "event_status": "completed",
    "payload_version": "1.0",
    "metadata": {"currency": "USD", "campaign": "winter"},
}

INVALID_SAMPLES = [
    {**VALID_SAMPLE, "event_id": None},
    {**VALID_SAMPLE, "event_timestamp": "not-a-timestamp"},
    {**VALID_SAMPLE, "event_type": "unknown"},
    {**VALID_SAMPLE, "event_value": -10},
    {**VALID_SAMPLE, "metadata": {}},
    {**VALID_SAMPLE, "payload_version": "9.9"},
]

# Quarantine and replay

The quarantine topic is an isolation boundary, not a trash can. Every invalid message retains its original payload and enough context to investigate the producer. A bounded local index supports the API, but Kafka is authoritative. Alerts should watch both absolute quarantine volume and rate.

The replay manager rejects ineligible or exhausted messages into `events.dead_letter`. For eligible records, it parses and revalidates the original payload. Still-invalid records receive an incremented retry count and latest error. Valid records receive `_replay` audit metadata, are published to `events.replay`, and then re-enter `events.raw` so replay cannot bypass validation, deduplication, or transformations.

Use a dry run first:

```bash
curl -X POST http://localhost:8000/replay/quarantine \
  -H "Content-Type: application/json" \
  -d '{"limit":100,"dry_run":true}'
```

Repair tooling is intentionally out of scope because field corrections should be domain-owned and auditable. At scale, use a compacted case-management topic or database rather than the local JSON index.

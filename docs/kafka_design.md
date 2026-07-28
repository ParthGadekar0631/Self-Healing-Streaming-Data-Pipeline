# Kafka design

`events.raw` is the only normal producer entry point. It has three partitions in the local stack, so a stable event ID key distributes records while preserving per-key order. `events.validated` is reserved for teams that want a validation boundary; the current pipeline proceeds directly to `events.transformed`. Transformed and aggregate topics let downstream consumers subscribe without reading storage files.

Invalid records are never silently dropped. `events.quarantine` carries the original string plus an error type, message, failed field, processing time, replay flag, and retry count. Operators can correct an eligible record and submit it through `events.replay`; the manager mirrors valid replay payloads into `events.raw` for the same validation path. Records that are malformed beyond repair, ineligible, or exceed the configured replay count go to `events.dead_letter`.

Local replication factor is one. Production topics should use at least three replicas, `min.insync.replicas=2`, appropriate retention/compaction, TLS/SASL, producer idempotence, and consumer-lag alerts. Dead-letter retention should match governance requirements. Payload compatibility should be enforced by a schema registry rather than deployment-only JSON Schema.

# System architecture

The data plane begins with JSON events in `events.raw`. Spark retains Kafka topic, partition, offset, timestamp, and the original string before parsing it against an explicit schema. Column expressions assign a single primary validation error, making routing deterministic and avoiding Python UDF overhead. Invalid rows become a quarantine envelope. Valid rows are normalized, watermark-deduplicated, and consumed by independent clean and aggregate sinks.

Clean writes use `foreachBatch` because cross-restart idempotency and multi-sink coordination need driver-side control. The batch claims unseen IDs in Redis (SQLite is the degraded-mode fallback), writes partitioned Parquet, publishes transformed JSON, and marks IDs `written`. The aggregate query uses event-time five-minute windows and a watermark so Spark can evict closed state. Each query has its own checkpoint; a failed sink cannot corrupt another query's progress.

The control plane consists of the failure detector, retry manager, checkpoint/recovery actions, incident collector, summarizer, replay manager, and FastAPI service. Operational state is written atomically to bounded JSON indexes in `data/metadata`; Kafka and Parquet remain the event and analytical systems of record.

## Delivery semantics

Kafka source offsets and Spark checkpoints provide replayable at-least-once input. Watermark deduplication removes duplicates within retained Spark state. Redis extends idempotency across checkpoint replacement and restarts. Parquet itself has no transaction log, so the design approximates effectively-once event writes. In a production implementation, an Iceberg, Delta, or Hudi table should replace raw Parquet when strict atomic commit semantics are required.

## Trust boundaries

Secrets come from environment variables or Kubernetes Secrets, never source control. Local Compose uses plaintext listeners only for developer convenience. Production Kafka should use TLS/SASL, restricted topic ACLs, encrypted object storage, Redis authentication/TLS, and network policies between the pipeline and its dependencies.

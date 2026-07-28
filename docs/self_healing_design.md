# Self-healing design

The failure detector converts exception evidence into Kafka, Redis, checkpoint, Spark query, output write, quarantine-rate, or unknown categories. Recoverable failures enter a bounded exponential retry. The supervisor stops surviving sibling queries before restarting the complete query set, preventing multiple active consumers in one process.

Each query owns a checkpoint leaf. Healthy checkpoints resume source offsets and state. An explicit `_CORRUPT` marker triggers `RecoveryActions.archive_corrupt_checkpoint`, which moves only that named leaf to a timestamped archive and recreates it. It refuses checkpoint roots and missing targets. Unmarked ambiguous corruption remains operator-gated because automatic deletion of uncertain state is too risky.

Redis uses atomic `SET NX EX` claims and TTL-backed lifecycle status. During Redis outages, a local SQLite store preserves per-instance idempotency. This allows temporary progress but cannot coordinate several pipeline replicas; production policy should either pause consumption or use a shared fallback. Failed record replay is capped and routes exhausted records to dead letter.

Every recovery attempt is appended to a bounded history and significant failures generate an incident report. Recovery is self-healing only for known transient cases; unknown failures terminate so orchestration and humans can intervene.

# Resume bullets

**Self-Healing Streaming Data Pipeline**  
Personal Project | Python, Apache Kafka, PySpark Structured Streaming, Redis, Parquet, Docker, Kubernetes

- Built a streaming pipeline that validates, transforms, deduplicates, and aggregates events using Kafka and PySpark.
- Enforced schema and quality rules, routing malformed records to quarantine topics with error context and replay support.
- Implemented automated failure detection, checkpoint recovery, exponential retries, and idempotent processing across restarts.
- Added an AI-assisted incident module that summarizes failures and recommends recovery actions from logs and pipeline metadata.

Interview talking points: event-time watermarks bound state; Redis extends deduplication across restarts; quarantine keeps poison messages observable; independent checkpoints isolate sinks; and mock-default AI makes the control plane testable and cost-free.

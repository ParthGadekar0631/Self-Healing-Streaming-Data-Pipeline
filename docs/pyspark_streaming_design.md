# PySpark Structured Streaming design

The source uses Spark's Kafka connector and stores source coordinates alongside `raw_payload`. `from_json` applies a stable `StructType`; a null parsed struct indicates malformed JSON. Validation is implemented with Spark SQL expressions so Catalyst can optimize the plan. The valid branch converts timestamps, derives partitions, normalizes dimensions, and adds a deterministic SHA-256 processing key.

`withWatermark("event_timestamp", "10 minutes").dropDuplicates(["event_id"])` bounds duplicate state. The aggregation branch groups by a five-minute event-time window, `event_type`, `region`, and `source_system`, then computes count, average value, error count, and approximate unique devices. Append mode emits windows only after the watermark passes them.

Three independently checkpointed queries write clean rows, quarantine envelopes, and aggregates. `foreachBatch` enables Redis claims, Kafka publishing, static Parquet partition writes, and batch metrics. Backpressure is left to Spark's micro-batch scheduler in the example; production tuning should set Kafka offset limits, state-store metrics, shuffle partitions, and executor sizing from measured throughput.

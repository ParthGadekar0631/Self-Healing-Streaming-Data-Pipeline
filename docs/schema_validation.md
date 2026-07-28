# Schema and data-quality validation

The canonical field contract lives in `schemas/event_schema.json`, the Pydantic model in `src/streaming/schema_registry.py`, and the Spark `StructType` returned by `spark_event_schema()`. `configs/quality_rules.yaml` is the operator-readable rule inventory. Pure-Python validation is used for unit tests and replay; Spark column validation handles the stream.

Rules require `event_id`, event and ingestion timestamps, source, region, payload version, and non-empty metadata. `event_type`, region, and version use allowlists. At least one of user or device ID must exist. Values must be numeric after parsing and cannot be negative except for an `error_event`. Event time must parse and cannot exceed the configured future tolerance.

Validation emits a primary `error_type`, `error_message`, and `failed_field`. Malformed JSON is not replay eligible without manual repair. Schema and quality failures can be replayed after correction. A production schema registry should additionally enforce backward/forward compatibility before a producer deployment.

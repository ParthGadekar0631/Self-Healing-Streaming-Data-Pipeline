"""End-to-end self-healing PySpark Structured Streaming application."""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from pathlib import Path
from typing import Any

from src.config import Settings, get_settings, load_yaml
from src.incident_ai.incident_collector import IncidentCollector
from src.incident_ai.incident_summarizer import IncidentSummarizer
from src.recovery.failure_detector import FailureDetector
from src.recovery.idempotency import IdempotencyStore
from src.recovery.recovery_actions import RecoveryActions
from src.recovery.retry_manager import RetryManager
from src.streaming.aggregations import build_aggregations
from src.streaming.checkpointing import CheckpointManager
from src.streaming.deduplication import watermark_deduplicate
from src.streaming.quarantine import quarantine_dataframe
from src.streaming.schema_registry import spark_event_schema
from src.streaming.spark_session import create_spark_session
from src.streaming.transformations import transform_dataframe
from src.streaming.validation import with_validation_columns
from src.utils.file_utils import atomic_write_json, read_json
from src.utils.logger import configure_logging
from src.utils.time_utils import utc_now_iso

logger = logging.getLogger(__name__)


class StreamingPipeline:
    def __init__(self, settings: Settings | None = None, spark=None) -> None:
        self.settings = settings or get_settings()
        self.spark = spark or create_spark_session(self.settings)
        self.checkpoints = CheckpointManager(self.settings.checkpoint_dir)
        self.idempotency = IdempotencyStore(self.settings)
        self.detector = FailureDetector(self.settings.quarantine_rate_alert_threshold)
        self.retry = RetryManager.from_settings(self.settings)
        self.recovery = RecoveryActions(self.settings.metadata_dir)
        self.collector = IncidentCollector(
            self.settings.metadata_dir,
            self.settings.parquet_output_dir / "incident_logs",
        )
        self.summarizer = IncidentSummarizer(self.settings)
        self.queries: list[Any] = []
        self._stopping = False
        self._metadata_lock = threading.Lock()
        config = load_yaml("pipeline_config.yaml")
        self.trigger_interval = config["pipeline"].get("trigger_interval", "10 seconds")

    def source(self):
        from pyspark.sql import functions as F

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("subscribe", self.settings.kafka_raw_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .select(
                F.col("value").cast("string").alias("raw_payload"),
                F.col("topic").alias("source_topic"),
                F.col("partition").alias("source_partition"),
                F.col("offset").alias("source_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
        )

    def build_frames(self):
        from pyspark.sql import functions as F

        raw = self.source()
        parsed = (
            raw.withColumn("_parsed", F.from_json("raw_payload", spark_event_schema()))
            .select("*", "_parsed.*")
        )
        checked = with_validation_columns(
            parsed, self.settings.future_timestamp_tolerance_minutes
        )
        invalid = quarantine_dataframe(checked.filter(~F.col("is_valid")))
        valid = checked.filter(F.col("is_valid")).drop(
            "_parsed", "is_valid", "error_type", "error_message", "failed_field"
        )
        transformed = watermark_deduplicate(
            transform_dataframe(valid), self.settings.dedup_watermark
        )
        aggregates = build_aggregations(transformed, self.settings.aggregation_window)
        return transformed, invalid, aggregates

    def _write_clean_batch(self, batch, batch_id: int) -> None:
        from pyspark.sql import functions as F

        if batch.isEmpty():
            self._update_metrics(batch_id, valid=0)
            return
        event_ids = [row.event_id for row in batch.select("event_id").distinct().collect()]
        claimed: list[str] = []
        for event_id in event_ids:
            status = self.idempotency.get_status(event_id)
            if status == "written":
                continue
            # A non-final status represents an interrupted earlier batch and is safe
            # to resume. A missing status is atomically claimed.
            if status is not None or self.idempotency.claim(event_id):
                claimed.append(event_id)
        if not claimed:
            logger.info("Batch %s contains only previously processed records", batch_id)
            self._update_metrics(batch_id, valid=0, duplicates=len(event_ids))
            return
        writable = batch.filter(F.col("event_id").isin(claimed)).cache()
        count = writable.count()
        try:
            for event_id in claimed:
                self.idempotency.set_status(event_id, "transformed")
            (
                writable.write.mode("append")
                .partitionBy("event_date", "region")
                .parquet(str(self.settings.parquet_output_dir / "clean_events"))
            )
            kafka_payload = writable.select(
                F.col("event_id").cast("string").alias("key"),
                F.to_json(
                    F.struct(*[F.col(name) for name in writable.columns])
                ).alias("value"),
            )
            (
                kafka_payload.write.format("kafka")
                .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
                .option("topic", self.settings.kafka_transformed_topic)
                .save()
            )
            for event_id in claimed:
                self.idempotency.set_status(event_id, "written")
            self._update_metrics(
                batch_id, valid=count, duplicates=max(0, len(event_ids) - len(claimed))
            )
            logger.info("Clean batch written", extra={"batch_id": batch_id})
        finally:
            writable.unpersist()

    def _write_quarantine_batch(self, batch, batch_id: int) -> None:
        from pyspark.sql import functions as F

        if batch.isEmpty():
            return
        count = batch.count()
        encoded = batch.select(
            F.lit(None).cast("string").alias("key"),
            F.to_json(F.struct(*[F.col(name) for name in batch.columns])).alias("value"),
        )
        (
            encoded.write.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("topic", self.settings.kafka_quarantine_topic)
            .save()
        )
        # A bounded local index gives the API immediate visibility without becoming
        # the system of record (Kafka remains authoritative).
        sampled = [row.asDict(recursive=True) for row in batch.limit(1_000).collect()]
        for row in batch.select("event_id").where("event_id IS NOT NULL").distinct().collect():
            if self.idempotency.get_status(row.event_id) is None:
                self.idempotency.set_status(row.event_id, "quarantined")
        records_path = self.settings.metadata_dir / "quarantine_records.json"
        with self._metadata_lock:
            existing = read_json(records_path, []) or []
            atomic_write_json(records_path, (existing + sampled)[-10_000:])
        self._update_metrics(batch_id, quarantined=count)
        logger.warning(
            "Quarantined invalid records", extra={"batch_id": batch_id, "action": "quarantine"}
        )

    def _write_aggregate_batch(self, batch, batch_id: int) -> None:
        from pyspark.sql import functions as F

        if batch.isEmpty():
            return
        (
            batch.write.mode("append")
            .partitionBy("window_date", "event_type")
            .parquet(str(self.settings.parquet_output_dir / "aggregates"))
        )
        encoded = batch.select(
            F.concat_ws(
                "|", F.col("window_start"), F.col("event_type"), F.col("region")
            ).alias("key"),
            F.to_json(F.struct(*[F.col(name) for name in batch.columns])).alias("value"),
        )
        (
            encoded.write.format("kafka")
            .option("kafka.bootstrap.servers", self.settings.kafka_bootstrap_servers)
            .option("topic", self.settings.kafka_aggregated_topic)
            .save()
        )
        self._update_metrics(batch_id, aggregate_rows=batch.count())

    def _update_metrics(self, batch_id: int, **increments: int) -> None:
        path = self.settings.metadata_dir / "pipeline_metrics.json"
        with self._metadata_lock:
            metrics = read_json(path, {}) or {}
            for name, amount in increments.items():
                metrics[name] = int(metrics.get(name, 0)) + amount
            metrics.update(
                {
                    "last_batch_id": batch_id,
                    "last_updated": utc_now_iso(),
                    "redis_status": (
                        "fallback" if self.idempotency.using_fallback else "healthy"
                    ),
                }
            )
            valid = int(metrics.get("valid", 0))
            invalid = int(metrics.get("quarantined", 0))
            total = valid + invalid
            metrics["quarantine_rate"] = invalid / total if total else 0.0
            atomic_write_json(path, metrics)

            rate_failure = self.detector.quarantine_rate_failure(invalid, total)
            already_alerted = bool(metrics.get("quarantine_rate_alerted"))
            if rate_failure and not already_alerted:
                context = self.collector.from_failure(rate_failure.to_dict())
                self.collector.persist_report(self.summarizer.summarize(context))
                metrics["quarantine_rate_alerted"] = True
                atomic_write_json(path, metrics)
            elif not rate_failure and already_alerted:
                metrics["quarantine_rate_alerted"] = False
                atomic_write_json(path, metrics)

    def start(self) -> list[Any]:
        for query_name in self.checkpoints.QUERY_NAMES:
            checkpoint = self.checkpoints.path_for(query_name)
            issue = self.detector.checkpoint_issue(checkpoint)
            if issue and issue.metadata.get("reason") == "corrupt":
                self.recovery.archive_corrupt_checkpoint(checkpoint)
        transformed, invalid, aggregates = self.build_frames()
        started: list[Any] = []
        try:
            started.append(
                transformed.writeStream.queryName("clean-events")
                .foreachBatch(self._write_clean_batch)
                .option(
                    "checkpointLocation", str(self.checkpoints.path_for("clean"))
                )
                .trigger(processingTime=self.trigger_interval)
                .start()
            )
            started.append(
                invalid.writeStream.queryName("quarantine-events")
                .foreachBatch(self._write_quarantine_batch)
                .option(
                    "checkpointLocation", str(self.checkpoints.path_for("quarantine"))
                )
                .trigger(processingTime=self.trigger_interval)
                .start()
            )
            started.append(
                aggregates.writeStream.queryName("aggregate-events")
                .outputMode("append")
                .foreachBatch(self._write_aggregate_batch)
                .option(
                    "checkpointLocation", str(self.checkpoints.path_for("aggregates"))
                )
                .trigger(processingTime=self.trigger_interval)
                .start()
            )
        except Exception:
            for query in started:
                if query.isActive:
                    query.stop()
            raise
        self.queries = started
        atomic_write_json(
            self.settings.metadata_dir / "pipeline_status.json",
            {
                "status": "running",
                "started_at": utc_now_iso(),
                "queries": [query.name for query in self.queries],
            },
        )
        logger.info("Started %s streaming queries", len(self.queries))
        return self.queries

    def stop(self) -> None:
        self._stopping = True
        for query in self.queries:
            if query.isActive:
                query.stop()
        atomic_write_json(
            self.settings.metadata_dir / "pipeline_status.json",
            {"status": "stopped", "stopped_at": utc_now_iso()},
        )

    def run_forever(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        signal.signal(signal.SIGINT, lambda *_: self.stop())
        while not self._stopping:
            try:
                self.retry.call(self.start)
                while not self._stopping and all(query.isActive for query in self.queries):
                    time.sleep(2)
                failures = [query.exception() for query in self.queries if query.exception()]
                if failures and not self._stopping:
                    raise RuntimeError("; ".join(str(item) for item in failures))
            except Exception as exc:
                failure = self.detector.classify(exc)
                incident_input = self.collector.from_failure(failure.to_dict())
                report = self.summarizer.summarize(incident_input)
                self.collector.persist_report(report)
                self.recovery.record(
                    "pipeline_supervisor_restart",
                    False,
                    failure=failure.to_dict(),
                    incident_id=report.get("incident_id"),
                )
                if not failure.recoverable or self._stopping:
                    raise
                logger.exception("Recoverable pipeline failure; supervisor will restart")
                for query in self.queries:
                    if query.isActive:
                        query.stop()
                time.sleep(self.settings.retry_base_delay_seconds)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    StreamingPipeline(settings).run_forever()


if __name__ == "__main__":
    main()

"""Spark session construction with Kafka and resilient state-store defaults."""

from __future__ import annotations

from src.config import Settings, load_yaml


def create_spark_session(settings: Settings):
    from pyspark.sql import SparkSession

    config = load_yaml("pipeline_config.yaml")
    spark_config = config.get("spark", {})
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_config.get("shuffle_partitions", 8)),
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    # Docker images install the Kafka connector. For a local pip Spark install, packages
    # can be resolved by setting PYSPARK_SUBMIT_ARGS as documented in the deployment guide.
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

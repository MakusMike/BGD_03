import logging
import os
import sys

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))

from settings import Settings
from ingest import run_ingest
from transform import run_transform
from aggregate import run_aggregate


def build_spark(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
        .getOrCreate()
    )


@task(name="bronze-ingest", retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def task_ingest(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting bronze layer: ingest from Kafka topic '%s'", settings.kafka_topic)
    run_ingest(spark, settings)
    logger.info("Bronze layer complete")


@task(name="silver-transform", retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def task_transform(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting silver layer: transform")
    run_transform(spark, settings)
    logger.info("Silver layer complete")


@task(name="gold-aggregate", retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def task_aggregate(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting gold layer: aggregate")
    run_aggregate(spark, settings)
    logger.info("Gold layer complete")


@flow(
    name="ddos-pipeline",
    description="DDoS detection pipeline: Kafka → Bronze → Silver → Gold",
    
    log_prints=True,
)
def ddos_pipeline_flow() -> None:
    settings = Settings()
    settings.ensure_dirs()

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    try:
        task_ingest(spark, settings)
        task_transform(spark, settings)
        task_aggregate(spark, settings)
    finally:
        spark.stop()


if __name__ == "__main__":
    ddos_pipeline_flow()
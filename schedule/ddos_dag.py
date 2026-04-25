from prefect import flow, task, get_run_logger
from prefect.client.schemas.schedules import CronSchedule
from pyspark.sql import SparkSession
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))

from pipeline.settings import Settings
from pipeline.ingest import run_ingest
from pipeline.transform import run_transform
from pipeline.aggregate import run_aggregate


def build_spark(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


@task(name="bronze-ingest", retries=2, retry_delay_seconds=30)
def task_ingest(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting bronze layer: ingest")
    run_ingest(spark, settings)
    logger.info("Bronze layer complete")


@task(name="silver-transform", retries=2, retry_delay_seconds=30)
def task_transform(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting silver layer: transform")
    run_transform(spark, settings)
    logger.info("Silver layer complete")


@task(name="gold-aggregate", retries=2, retry_delay_seconds=30)
def task_aggregate(spark: SparkSession, settings: Settings) -> None:
    logger = get_run_logger()
    logger.info("Starting gold layer: aggregate")
    run_aggregate(spark, settings)
    logger.info("Gold layer complete")


@flow(
    name="ddos-pipeline",
    description="DDoS detection pipeline: Bronze → Silver → Gold",
    schedule=CronSchedule(cron="0 2 * * *"),  # codziennie o 2:00
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

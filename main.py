import logging
import sys

from pyspark.sql import SparkSession

from settings import Settings
from ingest import run_ingest
from transform import run_transform
from aggregate import run_aggregate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ddos_pipeline")


def build_spark(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main() -> None:
    settings = Settings()
    settings.ensure_dirs()

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("=== Bronze: ingest ===")
        run_ingest(spark, settings)

        logger.info("=== Silver: transform ===")
        run_transform(spark, settings)

        logger.info("=== Gold: aggregate ===")
        run_aggregate(spark, settings)

        logger.info("Pipeline finished successfully ✓")
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
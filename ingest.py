import logging

from pyspark.sql import SparkSession, DataFrame

from settings import Settings

logger = logging.getLogger("ddos_pipeline.ingest")


def _read_raw_csv(spark: SparkSession, settings: Settings) -> DataFrame:
    logger.info("Reading source CSV: %s", settings.input_file)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("samplingRatio", "0.1")
        .csv(settings.input_file)
    )
    logger.info("Schema inferred — %d columns", len(df.columns))
    return df


def _drop_unused_columns(df: DataFrame, settings: Settings) -> DataFrame:
    cols_to_drop = [c for c in settings.drop_columns if c in df.columns]
    logger.info("Dropping unused columns: %s", cols_to_drop)
    return df.drop(*cols_to_drop)


def _write_bronze(df: DataFrame, settings: Settings) -> None:
    path = settings.bronze_path()
    logger.info("Writing bronze JSON to: %s", path)
    df.write.mode("overwrite").json(path)
    logger.info("Bronze write complete")


def run_ingest(spark: SparkSession, settings: Settings) -> None:
    df = _read_raw_csv(spark, settings)
    df = _drop_unused_columns(df, settings)
    _write_bronze(df, settings)
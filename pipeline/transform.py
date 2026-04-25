import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from settings import Settings

logger = logging.getLogger("ddos_pipeline.transform")


def _read_bronze(spark: SparkSession, settings: Settings) -> DataFrame:
    path = settings.bronze_path()
    logger.info("Reading bronze JSON from: %s", path)
    return spark.read.json(path)


def _replace_inf_with_null(df: DataFrame, settings: Settings) -> DataFrame:
    numeric_cols = [c for c, t in df.dtypes if t in settings.inf_prone_dtypes]
    logger.info("Replacing ±inf with null in %d numeric columns", len(numeric_cols))
    for col_name in numeric_cols:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).isin([float("inf"), float("-inf")]), None)
             .otherwise(F.col(col_name)),
        )
    return df


def _fill_nulls_with_mean(df: DataFrame, settings: Settings) -> DataFrame:
    numeric_cols = [c for c, t in df.dtypes if t in settings.inf_prone_dtypes]
    means = df.select([F.mean(c).alias(c) for c in numeric_cols]).collect()[0]
    fill_map = {c: means[c] for c in numeric_cols if means[c] is not None}
    logger.info("Filling nulls with column means (%d columns affected)", len(fill_map))
    return df.fillna(fill_map)


def _drop_duplicates(df: DataFrame) -> DataFrame:
    df_deduped = df.dropDuplicates()
    logger.info("Duplicate rows removed")
    return df_deduped


def _write_silver(df: DataFrame, settings: Settings) -> None:
    path = settings.silver_path()
    logger.info("Writing silver JSON to: %s", path)
    df.write.mode("overwrite").json(path)
    logger.info("Silver write complete")


def run_transform(spark: SparkSession, settings: Settings) -> None:
    df = _read_bronze(spark, settings)
    df = _replace_inf_with_null(df, settings)
    df = _fill_nulls_with_mean(df, settings)
    df = _drop_duplicates(df)
    _write_silver(df, settings)
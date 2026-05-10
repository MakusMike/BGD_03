import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from settings import Settings

logger = logging.getLogger("ddos_pipeline.transform")


def _read_bronze(spark: SparkSession, settings: Settings) -> DataFrame:
    path = settings.bronze_path()
    logger.info("Reading bronze JSON from: %s", path)
    return spark.read.json(path)


def _cast_columns(df: DataFrame) -> DataFrame:
    int_cols = [
        "Src Port", "Dst Port", "Protocol",
        "Tot Fwd Pkts", "Tot Bwd Pkts",
        "Fwd PSH Flags", "Bwd PSH Flags", "Fwd URG Flags", "Bwd URG Flags",
        "Fwd Header Len", "Bwd Header Len",
        "FIN Flag Cnt", "SYN Flag Cnt", "RST Flag Cnt", "PSH Flag Cnt",
        "ACK Flag Cnt", "URG Flag Cnt", "CWE Flag Count", "ECE Flag Cnt",
        "Fwd Byts/b Avg", "Fwd Pkts/b Avg", "Fwd Blk Rate Avg",
        "Bwd Byts/b Avg", "Bwd Pkts/b Avg", "Bwd Blk Rate Avg",
        "Subflow Fwd Pkts", "Subflow Fwd Byts",
        "Subflow Bwd Pkts", "Subflow Bwd Byts",
        "Init Fwd Win Byts", "Init Bwd Win Byts",
        "Fwd Act Data Pkts", "Fwd Seg Size Min",
    ]
    long_cols = ["Flow Duration"]
    double_cols = [
        "TotLen Fwd Pkts", "TotLen Bwd Pkts",
        "Fwd Pkt Len Max", "Fwd Pkt Len Min", "Fwd Pkt Len Mean", "Fwd Pkt Len Std",
        "Bwd Pkt Len Max", "Bwd Pkt Len Min", "Bwd Pkt Len Mean", "Bwd Pkt Len Std",
        "Flow Byts/s", "Flow Pkts/s",
        "Flow IAT Mean", "Flow IAT Std", "Flow IAT Max", "Flow IAT Min",
        "Fwd IAT Tot", "Fwd IAT Mean", "Fwd IAT Std", "Fwd IAT Max", "Fwd IAT Min",
        "Bwd IAT Tot", "Bwd IAT Mean", "Bwd IAT Std", "Bwd IAT Max", "Bwd IAT Min",
        "Fwd Pkts/s", "Bwd Pkts/s",
        "Pkt Len Min", "Pkt Len Max", "Pkt Len Mean", "Pkt Len Std", "Pkt Len Var",
        "Down/Up Ratio", "Pkt Size Avg", "Fwd Seg Size Avg", "Bwd Seg Size Avg",
        "Active Mean", "Active Std", "Active Max", "Active Min",
        "Idle Mean", "Idle Std", "Idle Max", "Idle Min",
    ]

    existing = set(df.columns)
    for col in int_cols:
        if col in existing:
            df = df.withColumn(col, F.col(col).cast(IntegerType()))
    for col in long_cols:
        if col in existing:
            df = df.withColumn(col, F.col(col).cast(LongType()))
    for col in double_cols:
        if col in existing:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    logger.info("Column types cast: %d int, %d long, %d double",
                len(int_cols), len(long_cols), len(double_cols))
    return df


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
    df = _cast_columns(df)
    df = _replace_inf_with_null(df, settings)
    df = _fill_nulls_with_mean(df, settings)
    df = _drop_duplicates(df)
    _write_silver(df, settings)
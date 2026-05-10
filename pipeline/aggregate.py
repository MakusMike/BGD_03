import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from settings import Settings

logger = logging.getLogger("ddos_pipeline.aggregate")


def _read_silver(spark: SparkSession, settings: Settings) -> DataFrame:
    path = settings.silver_path()
    logger.info("Reading silver JSON from: %s", path)
    return spark.read.json(path)


def _write_gold(df: DataFrame, table_name: str, settings: Settings) -> None:
    path = settings.gold_path(table_name)
    logger.info("Writing gold table '%s' to: %s", table_name, path)
    df.write.mode("overwrite").json(path)


def _top_source_ips(df: DataFrame, settings: Settings) -> DataFrame:
    return (
        df.filter(F.col(settings.label_column) == settings.label_attack)
        .groupBy(F.col(settings.src_ip_column).alias("src_ip"))
        .agg(F.count("*").alias("attack_count"))
        .orderBy(F.desc("attack_count"))
        .limit(20)
    )


def _traffic_by_label(df: DataFrame, settings: Settings) -> DataFrame:
    return (
        df.groupBy(F.col(settings.label_column).alias("label"))
        .agg(
            F.count("*").alias("total_flows"),
            F.sum(F.col(settings.fwd_pkts_column)).alias("total_fwd_packets"),
            F.sum(F.col(settings.bwd_pkts_column)).alias("total_bwd_packets"),
        )
        .orderBy("label")
    )


def _attack_rate_by_port(df: DataFrame, settings: Settings) -> DataFrame:
    return (
        df.groupBy(F.col(settings.dst_port_column).alias("dst_port"))
        .agg(
            F.count("*").alias("total_flows"),
            F.sum(
                F.when(F.col(settings.label_column) == settings.label_attack, 1).otherwise(0)
            ).alias("attack_flows"),
        )
        .withColumn(
            "attack_rate",
            F.round(F.col("attack_flows") / F.col("total_flows"), 4),
        )
        .orderBy(F.desc("attack_rate"))
        .limit(20)
    )


def _flow_duration_stats(df: DataFrame, settings: Settings) -> DataFrame:
    return (
        df.groupBy(F.col(settings.label_column).alias("label"))
        .agg(
            F.round(F.mean(F.col(settings.flow_duration_column)), 2).alias("mean_duration_us"),
            F.min(F.col(settings.flow_duration_column)).alias("min_duration_us"),
            F.max(F.col(settings.flow_duration_column)).alias("max_duration_us"),
        )
    )


def run_aggregate(spark: SparkSession, settings: Settings) -> None:
    df = _read_silver(spark, settings)
    df.cache()

    aggregations = {
        "top_source_ips":      _top_source_ips(df, settings),
        "traffic_by_label":    _traffic_by_label(df, settings),
        "attack_rate_by_port": _attack_rate_by_port(df, settings),
        "flow_duration_stats": _flow_duration_stats(df, settings),
    }

    for table_name, result_df in aggregations.items():
        _write_gold(result_df, table_name, settings)

    df.unpersist()
    logger.info("Gold layer complete — %d tables written", len(aggregations))
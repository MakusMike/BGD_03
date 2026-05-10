import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

from settings import Settings

logger = logging.getLogger("ddos_pipeline.ingest")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "ddos-raw")

_BRONZE_SCHEMA = StructType([
    StructField("Unnamed: 0",         StringType(),  True),  # dropped — CSV artefact
    StructField("Flow ID",            StringType(),  True),  # dropped — composite key string
    StructField("Src IP",             StringType(),  True),
    StructField("Src Port",           StringType(),  True),
    StructField("Dst IP",             StringType(),  True),
    StructField("Dst Port",           StringType(),  True),
    StructField("Protocol",           StringType(),  True),
    StructField("Timestamp",          StringType(),  True),
    StructField("Flow Duration",      StringType(),  True),
    StructField("Tot Fwd Pkts",       StringType(),  True),
    StructField("Tot Bwd Pkts",       StringType(),  True),
    StructField("TotLen Fwd Pkts",    StringType(),  True),
    StructField("TotLen Bwd Pkts",    StringType(),  True),
    StructField("Fwd Pkt Len Max",    StringType(),  True),
    StructField("Fwd Pkt Len Min",    StringType(),  True),
    StructField("Fwd Pkt Len Mean",   StringType(),  True),
    StructField("Fwd Pkt Len Std",    StringType(),  True),
    StructField("Bwd Pkt Len Max",    StringType(),  True),
    StructField("Bwd Pkt Len Min",    StringType(),  True),
    StructField("Bwd Pkt Len Mean",   StringType(),  True),
    StructField("Bwd Pkt Len Std",    StringType(),  True),
    StructField("Flow Byts/s",        StringType(),  True),
    StructField("Flow Pkts/s",        StringType(),  True),
    StructField("Flow IAT Mean",      StringType(),  True),
    StructField("Flow IAT Std",       StringType(),  True),
    StructField("Flow IAT Max",       StringType(),  True),
    StructField("Flow IAT Min",       StringType(),  True),
    StructField("Fwd IAT Tot",        StringType(),  True),
    StructField("Fwd IAT Mean",       StringType(),  True),
    StructField("Fwd IAT Std",        StringType(),  True),
    StructField("Fwd IAT Max",        StringType(),  True),
    StructField("Fwd IAT Min",        StringType(),  True),
    StructField("Bwd IAT Tot",        StringType(),  True),
    StructField("Bwd IAT Mean",       StringType(),  True),
    StructField("Bwd IAT Std",        StringType(),  True),
    StructField("Bwd IAT Max",        StringType(),  True),
    StructField("Bwd IAT Min",        StringType(),  True),
    StructField("Fwd PSH Flags",      StringType(),  True),
    StructField("Bwd PSH Flags",      StringType(),  True),
    StructField("Fwd URG Flags",      StringType(),  True),
    StructField("Bwd URG Flags",      StringType(),  True),
    StructField("Fwd Header Len",     StringType(),  True),
    StructField("Bwd Header Len",     StringType(),  True),
    StructField("Fwd Pkts/s",         StringType(),  True),
    StructField("Bwd Pkts/s",         StringType(),  True),
    StructField("Pkt Len Min",        StringType(),  True),
    StructField("Pkt Len Max",        StringType(),  True),
    StructField("Pkt Len Mean",       StringType(),  True),
    StructField("Pkt Len Std",        StringType(),  True),
    StructField("Pkt Len Var",        StringType(),  True),
    StructField("FIN Flag Cnt",       StringType(),  True),
    StructField("SYN Flag Cnt",       StringType(),  True),
    StructField("RST Flag Cnt",       StringType(),  True),
    StructField("PSH Flag Cnt",       StringType(),  True),
    StructField("ACK Flag Cnt",       StringType(),  True),
    StructField("URG Flag Cnt",       StringType(),  True),
    StructField("CWE Flag Count",     StringType(),  True),
    StructField("ECE Flag Cnt",       StringType(),  True),
    StructField("Down/Up Ratio",      StringType(),  True),
    StructField("Pkt Size Avg",       StringType(),  True),
    StructField("Fwd Seg Size Avg",   StringType(),  True),
    StructField("Bwd Seg Size Avg",   StringType(),  True),
    StructField("Fwd Byts/b Avg",     StringType(),  True),
    StructField("Fwd Pkts/b Avg",     StringType(),  True),
    StructField("Fwd Blk Rate Avg",   StringType(),  True),
    StructField("Bwd Byts/b Avg",     StringType(),  True),
    StructField("Bwd Pkts/b Avg",     StringType(),  True),
    StructField("Bwd Blk Rate Avg",   StringType(),  True),
    StructField("Subflow Fwd Pkts",   StringType(),  True),
    StructField("Subflow Fwd Byts",   StringType(),  True),
    StructField("Subflow Bwd Pkts",   StringType(),  True),
    StructField("Subflow Bwd Byts",   StringType(),  True),
    StructField("Init Fwd Win Byts",  StringType(),  True),
    StructField("Init Bwd Win Byts",  StringType(),  True),  # NOTE: "Byts" not "Bytes"
    StructField("Fwd Act Data Pkts",  StringType(),  True),
    StructField("Fwd Seg Size Min",   StringType(),  True),
    StructField("Active Mean",        StringType(),  True),
    StructField("Active Std",         StringType(),  True),
    StructField("Active Max",         StringType(),  True),
    StructField("Active Min",         StringType(),  True),
    StructField("Idle Mean",          StringType(),  True),
    StructField("Idle Std",           StringType(),  True),
    StructField("Idle Max",           StringType(),  True),
    StructField("Idle Min",           StringType(),  True),
    StructField("Label",              StringType(),  True),
])


def _read_kafka_stream(spark: SparkSession) -> DataFrame:
    logger.info("Connecting to Kafka: %s, topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), _BRONZE_SCHEMA).alias("data"))
        .select("data.*")
    )

    logger.info("Kafka stream configured — %d fields in schema", len(_BRONZE_SCHEMA))
    return parsed


def _drop_unused_columns(df: DataFrame, settings: Settings) -> DataFrame:
    cols_to_drop = [c for c in settings.drop_columns if c in df.columns]
    logger.info("Dropping artefact columns: %s", cols_to_drop)
    return df.drop(*cols_to_drop)


def _write_bronze_stream(df: DataFrame, settings: Settings) -> None:
    checkpoint = settings.bronze_checkpoint_path()
    path       = settings.bronze_path()

    logger.info("Writing Bronze to: %s  (checkpoint: %s)", path, checkpoint)

    query = (
        df.writeStream
        .format("json")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    logger.info("Bronze write complete")


def run_ingest(spark: SparkSession, settings: Settings) -> None:
    df = _read_kafka_stream(spark)
    df = _drop_unused_columns(df, settings)
    _write_bronze_stream(df, settings)
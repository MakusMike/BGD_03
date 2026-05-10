import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


@dataclass
class Settings:

    input_file: str = field(
        default_factory=lambda: str(PROJECT_ROOT / "data" / "raw" / "dataset.csv")
    )
    bronze_dir: str = field(
        default_factory=lambda: str(PROJECT_ROOT / "data" / "bronze")
    )
    silver_dir: str = field(
        default_factory=lambda: str(PROJECT_ROOT / "data" / "silver")
    )
    gold_dir: str = field(
        default_factory=lambda: str(PROJECT_ROOT / "data" / "gold")
    )

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    kafka_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "ddos-raw")
    )

    spark_shuffle_partitions: int = 8
    spark_app_name: str = "ddos_pipeline"

    drop_columns: tuple = ("Unnamed: 0", "Flow ID")

    label_column:         str = "Label"
    src_ip_column:        str = "Src IP"
    src_port_column:      str = "Src Port"
    dst_ip_column:        str = "Dst IP"
    dst_port_column:      str = "Dst Port"
    protocol_column:      str = "Protocol"
    timestamp_column:     str = "Timestamp"
    flow_duration_column: str = "Flow Duration"
    fwd_pkts_column:      str = "Tot Fwd Pkts"
    bwd_pkts_column:      str = "Tot Bwd Pkts"
    flow_iat_min_column:  str = "Flow IAT Min"
    fwd_seg_size_column:  str = "Fwd Seg Size Avg"
    init_bwd_win_column:  str = "Init Bwd Win Byts"

    label_attack: str = "ddos"
    label_benign: str = "Benign"

    inf_prone_dtypes: tuple = ("float", "double")

    def bronze_path(self) -> str:
        return self.bronze_dir

    def bronze_checkpoint_path(self) -> str:
        return str(Path(self.bronze_dir).parent / "checkpoints" / "bronze")

    def silver_path(self) -> str:
        return self.silver_dir

    def gold_path(self, table_name: str) -> str:
        return str(Path(self.gold_dir) / table_name)

    def ensure_dirs(self) -> None:
        for dir_path in [
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.bronze_checkpoint_path(),
        ]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
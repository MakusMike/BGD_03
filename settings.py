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

    # Spark tuning
    spark_shuffle_partitions: int = 8
    spark_app_name: str = "ddos_pipeline"

    # Column names — matched to actual dataset
    label_column: str = "Label"
    label_attack: str = "ddos"
    label_benign: str = "Benign"
    src_ip_column: str = "Src IP"
    dst_port_column: str = "Dst Port"
    fwd_pkts_column: str = "Tot Fwd Pkts"
    bwd_pkts_column: str = "Tot Bwd Pkts"
    flow_duration_column: str = "Flow Duration"

    # Columns to drop on ingest (pandas index artifact + composite key not useful for analysis)
    drop_columns: tuple = ("Unnamed: 0", "Flow ID")

    inf_prone_dtypes: tuple = ("float", "double")

    def bronze_path(self) -> str:
        return self.bronze_dir

    def silver_path(self) -> str:
        return self.silver_dir

    def gold_path(self, table_name: str) -> str:
        return str(Path(self.gold_dir) / table_name)

    def ensure_dirs(self) -> None:
        for dir_path in (self.bronze_dir, self.silver_dir, self.gold_dir):
            Path(dir_path).mkdir(parents=True, exist_ok=True)
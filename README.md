# DDoS Pipeline

A distributed data processing pipeline built with Apache Spark that ingests, cleans, and aggregates network traffic data to identify DDoS attack patterns.

The pipeline follows a **medallion architecture** (Bronze > Silver > Gold) and is fully orchestrated via Docker Compose.

---

## Architecture shown in architecture.png

---

## Project Structure

```
project/
├── main.py               # Single entry point - orchestrates all layers
├── ingest.py             # Bronze: CSV > JSON
├── transform.py          # Silver: cleaning and deduplication
├── aggregate.py          # Gold: analytical aggregations
├── settings.py           # All configuration in one place
├── docker-compose.yml    # Spark cluster + pipeline runner
└── data/
    ├── raw/
    │   └── dataset.csv   # Input file (not committed to Git)
    ├── bronze/           # Raw data in JSON format
    ├── silver/           # Cleaned data in JSON format
    └── gold/             # Aggregated output tables
        ├── top_source_ips/
        ├── traffic_by_label/
        ├── attack_rate_by_port/
        └── flow_duration_stats/
```

---

## Requirements

- Docker
- Docker Compose
- The dataset CSV placed at `data/raw/dataset.csv`
- Download dataset from > https://www.kaggle.com/datasets/devendra416/ddos-datasets?resource=download
---

## How to Run

```bash
docker compose up pipeline
```

This will:
1. Start the Spark master
2. Start the Spark worker (waits for master to be healthy)
3. Run the pipeline (waits for worker to be healthy)

Spark UI is available at `http://localhost:8080` while the cluster is running.

To run again (pipeline is idempotent > safe to re-run, overwrites previous output):

```bash
docker compose up pipeline
```

---

## Gold Layer Outputs

| Table | Description |
|---|---|
| `top_source_ips` | Top 20 source IPs by attack flow count |
| `traffic_by_label` | Flow count and packet totals per label (ddos / Benign) |
| `attack_rate_by_port` | Top 20 destination ports by attack-flow rate |
| `flow_duration_stats` | Mean, min, max flow duration per label |

---

## Configuration

All settings are in `settings.py`. Key values you may want to change:

| Setting | Default | Description |
|---|---|---|
| `input_file` | `data/raw/dataset.csv` | Path to source CSV |
| `label_attack` | `ddos` | Attack label value in the dataset |
| `label_benign` | `Benign` | Benign label value in the dataset |
| `spark_shuffle_partitions` | `8` | Tune based on dataset size and available cores |
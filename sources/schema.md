# Data source: DDoS Network Traffic Dataset

## Origin

- **Source:** Kaggle - devendra416/ddos-datasets
- **Format:** CSV, delimiter `,`, header in first row
- **Size:** ~170 MB, ~225 000 rows
- **Classes:** `ddos` (attack traffic) / `Benign` (normal traffic)
- **Ingestion:** rows published to Kafka topic `ddos-raw` by `sources/producer.py`

## Key columns

| Column | Type | Description                                                            |
|---|---|------------------------------------------------------------------------|
| Label | string | Flow class: `ddos` / `Benign`                                          |
| Src IP | string | Source IP address                                                      |
| Dst Port | int | Destination port                                                       |
| Flow Duration | long | Flow duration in microseconds                                          |
| Tot Fwd Pkts | int | Total forward packets                                                  |
| Tot Bwd Pkts | int | Total backward packets                                                 |
| Flow ID | string | Dropped - unique flow identifier (no analytical value)                 |
| Unnamed: 0 | int | Dropped - CSV export artefact from `df.to_csv()` without `index=False` |

## Processing layers

| Layer | Description |
|---|---|
| Bronze | Kafka stream → JSON, drop unused columns |
| Silver | Replace `±Infinity` with `null`, impute column means, deduplicate |
| Gold | 4 analytical tables: top IPs, port attack rates, traffic by label, duration stats |

## Known data quality issues

- Float/double columns may contain `Infinity` values - handled in Silver by replacing with `null` and imputing the column mean.
- `Unnamed: 0` is a CSV export artefact - dropped in Bronze.
- All values arrive as strings from Kafka (JSON serialised by the producer) - type casting happens in Silver.
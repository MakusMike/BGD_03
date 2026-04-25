# DDoS Pipeline

Rozproszony pipeline do przetwarzania danych sieciowych zbudowany na Apache Spark,
wykrywający wzorce ataków DDoS w ruchu sieciowym.

Pipeline następuje architekturze **medallion** (Bronze → Silver → Gold)
i jest orkiestrowany przez **Prefect** z automatycznym harmonogramem.

---

## Architektura

![Pipeline Architecture](architecture.png)

```
Źródło CSV  →  Bronze (JSON)  →  Silver (czyszczenie)  →  Gold (agregacje)
                                                               ↑
                                                         Prefect flow
                                                      (schedule: cron 2:00)
```

---

## Struktura projektu

```
BGD_03/
├── schedule/
│   └── ddos_dag.py           # Prefect flow - główny punkt wejścia orkiestratora
├── pipeline/
│   ├── main.py               # Bezpośrednie uruchomienie bez orkiestratora
│   ├── ingest.py             # Bronze: CSV → JSON
│   ├── transform.py          # Silver: czyszczenie i deduplikacja
│   ├── aggregate.py          # Gold: tabele analityczne
│   └── settings.py           # Cała konfiguracja w jednym miejscu
├── sources/
│   ├── source_config.yml     # Parametry źródła danych
│   └── schema.md             # Opis schematu i warstw przetwarzania
├── tech/
│   └── docker-compose.yml    # Spark cluster + Prefect server + pipeline runner
├── data/
│   ├── raw/
│   │   └── dataset.csv       # Plik wejściowy (nie commitowany do Git)
│   ├── bronze/               # Surowe dane w formacie JSON
│   ├── silver/               # Dane po czyszczeniu
│   └── gold/                 # Tabele wynikowe
│       ├── top_source_ips/
│       ├── traffic_by_label/
│       ├── attack_rate_by_port/
│       └── flow_duration_stats/
├── architecture.png
├── requirements.txt
└── README.md
```

---

## Wymagania

- Docker + Docker Compose
- Plik datasetu CSV umieszczony w `data/raw/dataset.csv`
- Dataset do pobrania: https://www.kaggle.com/datasets/devendra416/ddos-datasets

---

## Uruchomienie

### Poprzez Prefect orkiestrator

```bash
docker compose -f tech/docker-compose.yml up
```

Uruchamia:
1. **Prefect server** - UI dostępne pod `http://localhost:4200`
2. **Spark master** - UI pod `http://localhost:8080`
3. **Spark worker** - czeka na zdrowy master
4. **Pipeline runner** - wykonuje `schedule/ddos_dag.py`, czeka na oba powyższe

Prefect automatycznie uruchamia pipeline codziennie o 2:00 (CronSchedule).
Historia runów, logi i statusy są widoczne w Prefect UI.

### Bezpośrednio bez orkiestratora

```bash
cd pipeline
python main.py
```

Pipeline jest **idempotentny** - bezpieczne wielokrotne uruchamianie, poprzednie wyniki są nadpisywane.

---

## Warstwy przetwarzania

| Warstwa | Plik | Opis |
|---------|------|------|
| Bronze | `ingest.py` | Wczytanie CSV, usunięcie zbędnych kolumn (`Unnamed: 0`, `Flow ID`), zapis do JSON |
| Silver | `transform.py` | Zamiana `±Infinity` na `null`, imputacja średnią kolumny, deduplikacja |
| Gold | `aggregate.py` | 4 tabele analityczne (patrz niżej) |

### Tabele Gold

| Tabela | Opis |
|--------|------|
| `top_source_ips` | Top 20 adresów IP źródłowych według liczby przepływów ataku |
| `traffic_by_label` | Liczba przepływów i pakietów dla każdej klasy (ddos / Benign) |
| `attack_rate_by_port` | Top 20 portów docelowych według wskaźnika ataków |
| `flow_duration_stats` | Średni, minimalny i maksymalny czas trwania przepływu per klasa |

---

## Konfiguracja

Wszystkie ustawienia w `pipeline/settings.py`. Parametry źródła danych opisane w `sources/source_config.yml`.

| Parametr | Domyślnie | Opis |
|----------|-----------|------|
| `input_file` | `data/raw/dataset.csv` | Ścieżka do pliku CSV |
| `label_attack` | `ddos` | Wartość etykiety ataku w datasecie |
| `label_benign` | `Benign` | Wartość etykiety ruchu normalnego |
| `spark_shuffle_partitions` | `8` | Dostosuj do rozmiaru danych i liczby rdzeni |
| `spark_app_name` | `ddos_pipeline` | Nazwa aplikacji Spark (widoczna w UI) |

---

## Orkiestracja (Prefect)

`schedule/ddos_dag.py` definiuje flow Prefect składający się z trzech tasków:

```
ddos_pipeline_flow
├── task_ingest      (Bronze, retries=2)
├── task_transform   (Silver, retries=2)
└── task_aggregate   (Gold,   retries=2)
```

Każdy task ma skonfigurowane 2 retry z 30-sekundowym opóźnieniem.
Flow jest zarejestrowany z harmonogramem `CronSchedule(cron="0 2 * * *")`.
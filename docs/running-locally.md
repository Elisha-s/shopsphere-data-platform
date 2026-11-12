# Running locally

## Requirements

- Python 3.10, 3.11 or 3.12
- Java 17 (`java -version`)
- Internet access on the first run, for Spark to download the Delta Lake
  libraries from Maven Central (cached in `~/.ivy2` afterwards)

Native Windows needs Hadoop's `winutils` for local Spark. Use WSL or the
Docker option instead.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Option A: files, no Kafka

```bash
# Two hours of events, generated instantly
python -m ingestion.run_producer --target files --max-events 5000 --backfill-minutes 120

# The whole pipeline, once
python -m local_pipeline.run_pipeline --source files

# A tour of the tables
python -m local_pipeline.show_results
```

The run ends with the validation report and row counts, for example:

```text
[PASS] bronze_equals_silver_plus_invalid: bronze rows=5081, unique events=5000 (duplicates removed=81), silver valid=4897, silver invalid=103
[PASS] current_orders_one_row_per_order: current_orders rows=2785, distinct order ids=2785, distinct order ids in silver=2785
[PASS] current_orders_hold_latest_state: orders whose stored state is older than their newest event=0
...
```

Exact numbers differ per run; add `--seed 1` to the producer for a repeatable one.

### Showing incremental processing

```bash
python -m ingestion.run_producer --target files --max-events 1000 --backfill-minutes 20
python -m local_pipeline.run_pipeline --source files
```

Only the new files are ingested (streaming checkpoints), and the CDF merge
log line shows it reading only the new Delta versions.

### Starting over

```bash
python -m local_pipeline.run_pipeline --source files --reset   # rebuild tables from the landing files
rm -rf data                                                    # delete everything
```

## Option B: Kafka in Docker

```bash
docker compose -f local_pipeline/docker/docker-compose.yml up --build -d
docker compose -f local_pipeline/docker/docker-compose.yml logs -f pipeline
```

| Service | What it does |
|---|---|
| `kafka` | Single-node broker (KRaft) |
| `kafka-ui` | Topic browser on <http://localhost:8080> |
| `producer` | Sends five ticks of events a second to topic `orders` |
| `pipeline` | Runs all five stages every 30 seconds |

To see the tables:

```bash
docker compose -f local_pipeline/docker/docker-compose.yml stop pipeline
docker compose -f local_pipeline/docker/docker-compose.yml run --rm pipeline python -m local_pipeline.show_results
docker compose -f local_pipeline/docker/docker-compose.yml start pipeline
```

(The pipeline is stopped first because the embedded metastore allows one
process at a time.)

Stop and delete everything with `docker compose -f local_pipeline/docker/docker-compose.yml down -v`.

## Where the data lives

Everything is under `data/` (or the `shopsphere-data` Docker volume):

| Folder | Contents |
|---|---|
| `landing/orders/` | Input JSON files (file source only) |
| `warehouse/` | Delta tables, one folder per schema |
| `checkpoints/` | Streaming progress |
| `metastore_db/` | Table names and locations |

## Generator options

```text
--target files|kafka|eventhubs     where to send events
--max-events N                     stop after N ticks
--backfill-minutes M               spread event times over the last M minutes, as fast as possible
--rate R                           ticks per second in live mode (default 5)
--seed S                           reproducible run
--invalid-rate / --duplicate-rate / --late-rate    share of injected problem events
```

# Data coding exercise

A data exercise with a Postgres warehouse and dbt. Source data lives in Postgres (schema `raw`); initial data is loaded from `data/initial/`, and incremental batches are appended via `bin/ingest`. You will build dbt models in **staging**, **intermediate**, and **mart** layers from the provided sources.

**Prerequisites:** Docker.

---

## Candidate instructions and goals

Your task is to design and implement the dbt modeling layers so that the data is clean, well-structured, and ready for analysis.

### Data model head start

**`dbt/models/sources.yml`** defines the raw source tables and columns. Use it as your reference for the data model. Raw tables live in the `raw` schema and include:

- **accounts** – Stable account entity (account_id, email, created_at).
- **events** – Shows/productions (e.g. Wicked, Hamilton): event_id, name, slug.
- **showtimes** – A specific performance of an event: showtime_id, event_id, start_at.
- **orders** – Order header: order_id, account_id, showtime_id, created_at, total_amount.
- **transactions** – Payment records: transaction_id, order_id, amount, occurred_at.
- **pages** – Browsing behavior with **stable** `account_id` and **unstable** `customer_id` (may be merged over time); optional event_id, showtime_id.
- **identity_merges** – Merge log for customer_id (from_customer_id → to_customer_id, merged_at). Use to resolve pages to a canonical identity when building marts.

Raw data is intentionally varied (whitespace, casing, amount formats, sentinel values). See [docs/DATA_LIFECYCLE.md](docs/DATA_LIFECYCLE.md) for normalization expectations.

### What to build

1. **Staging** (`dbt/models/staging/`)  
   One or more models per raw source. Clean and normalize: trim text, standardize enums (e.g. page_type), cast amounts (strip `$` and commas) and timestamps, and treat sentinel values (`N/A`, `NULL`, empty) as SQL NULL. Staging output should be safe for downstream use.

2. **Intermediate** (`dbt/models/intermediate/`)  
   Models that sit between staging and marts: e.g. resolve pages to a canonical customer using `identity_merges`, or join orders to showtimes and events for analysis.

3. **Mart** (`dbt/models/mart/`)  
   Business-ready models that support queries such as **weekly aggregate sales** (e.g. sales by week, optionally by event or show). Use appropriate grain (e.g. one row per order or per transaction) and consider incremental materialization where it makes sense.

You choose the exact staging, intermediate, and mart models and their grain; the structure above and `sources.yml` are your starting point.

---

## Run the stack

From the repo root:

```bash
docker compose up -d
```

## Initial state and reset

Bring the warehouse to initial state (load source data, dbt seed, dbt run):

```bash
./scripts/init.sh
```

Reset to that state anytime:

```bash
./scripts/reset.sh
```

See [docs/DATA_LIFECYCLE.md](docs/DATA_LIFECYCLE.md) for init, reset, incremental ingestion, and raw data quality notes.

## Commands (bin shims)

Run these from the repo root. They wrap `docker compose run --rm ...`.

| Command | Purpose |
|--------|---------|
| `bin/dbt run` | Run dbt models |
| `bin/dbt seed` | Load dbt seeds (mappings) |
| `bin/dbt test` | Run dbt tests |
| `bin/dbt build` | Run models and tests |
| `bin/ingest` | Append `data/incremental/*` into source tables |
| `bin/ingest events/batch_001` | Ingest a single batch |
| `bin/load-initial` | Load `data/initial/*` into raw (used by init) |
| `bin/spark-submit example_job.py` | Submit a PySpark job |

## Project layout

| Path | Purpose |
|------|---------|
| `dbt/models/` | dbt models. `sources.yml` defines raw sources; `staging/`, `intermediate/`, `mart/` are where you add models. |
| `dbt/seeds/` | Mapping seeds (e.g. event_type_mapping.csv); use `ref()` in models. |
| `pyspark/` | PySpark scripts; run with `bin/spark-submit <script>.py`. |
| `data/initial/` | CSVs loaded into source tables at init. |
| `data/incremental/` | CSVs appended by `bin/ingest` (e.g. `events/batch_001.csv`). |
| `scripts/` | Init, reset, load_initial_source_data.py, ingest.py. |
| `bin/` | Shims for dbt, ingest, load-initial, spark-submit. |
| `docs/` | Data lifecycle and normalization expectations. |

## dbt

- **Sources** in `dbt/models/sources.yml` point at schema `raw`. Reference them with `{{ source('raw', 'table_name') }}`.
- **Models** go in `staging/`, `intermediate/`, and `mart/` under `dbt/models/`. Build from sources (and optionally seeds) in staging, then from staging/intermediate in later layers.
- **Seeds** in `dbt/seeds/` are for mappings only; use `ref()` to reference them.

No local dbt install required; use `bin/dbt run`, `bin/dbt seed`, `bin/dbt test`, etc.

## PySpark

Scripts in `pyspark/` connect to the warehouse via JDBC. Example:

```bash
bin/spark-submit example_job.py
```

**Warehouse:** host `warehouse`, port 5432, database `warehouse`, user/password `postgres`/`postgres`. JDBC driver is included; no `--packages` needed. To reduce logs, set `spark.sparkContext.setLogLevel("WARN")` in your script.

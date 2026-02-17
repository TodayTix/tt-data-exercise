# Data coding exercise

## Overview

For this round, you'll complete a short data engineering exercise using a containerized Postgres warehouse and dbt. The exercise runs locally via Docker, so please make sure Docker is installed before you begin. This repository contains source data, starter dbt project structure, and scripts. Your goal is to design and build the staging, intermediate, and mart layers so the data is clean, well-structured, and ready for analysis, based on the provided sources and normalization guidelines.

Source data lives in Postgres (schema `raw`); initial data is loaded from `data/initial/`, and incremental batches are appended via `bin/ingest`. You will build dbt models in **staging**, **intermediate**, and **mart** layers from the provided sources.

## Before You Begin

### 1. Verify Docker is installed and working

Run these commands to confirm your Docker setup:

```bash
docker --version
docker compose version
docker run --rm hello-world
docker compose ls
```

### 2. Start the stack and initialize the warehouse

From the repo root, run:

```bash
# Start Postgres and other services
docker compose up -d

# Load initial data and run dbt
./scripts/init.sh
```

### 3. Verify dbt works

```bash
bin/dbt --version
bin/dbt run
```

If these commands complete successfully, you're ready for the interview.

## What to Expect

The exercise will be completed **live with the interviewer(s)**. You'll work locally using this repo. You may use your normal tools, including AI assistants.

**This exercise is about reasoning and approach, not finishing everything.** You are not expected to model every table or build a full production-ready warehouse in the time provided. Focus on a sensible subset of the data, clear model structure, and sound assumptions and explanations.

---

## Your Assignment

### How to Approach This Exercise

Explore the source data and use your judgment to design dbt models that make the data analysis-ready. Consider the relationships, data quality, and potential business questions supported by the warehouse. Determine where to clean, transform, join, or standardize raw data, and decide which layers (staging, intermediate, mart) are most appropriate for each transformation.

Aim to build clear, well-documented, and testable outputs that illustrate your modeling choices. You are encouraged to adopt practices and structures you find suitable for delivering trustworthy, business-friendly data sets, making explicit any key definitions, grains, or assumptions in your work.

Focus on pragmatic modeling and testing strategies, and use documentation to share your rationale and any noteworthy challenges encountered.

### dbt Modeling Layers

1. **Staging** (`dbt/models/staging/`)
   One or more models per raw source. Clean and normalize: trim text, standardize enums (e.g. page_type), cast amounts (strip `$` and commas) and timestamps, and treat sentinel values (`N/A`, `NULL`, empty) as SQL NULL. Staging output should be safe for downstream use.

2. **Intermediate** (`dbt/models/intermediate/`)
   Models that sit between staging and marts: e.g. resolve pages to a canonical customer using `identity_merges`, or join orders to showtimes and events for analysis.

3. **Mart** (`dbt/models/mart/`)
   Business-ready models that support queries such as **weekly aggregate sales** (e.g. sales by week, optionally by event or show). Use appropriate grain (e.g. one row per order or per transaction) and consider incremental materialization where it makes sense.

---

## Data Model Reference

**`dbt/models/sources.yml`** defines the raw source tables and columns. Use it as your reference for the data model. Raw tables live in the `raw` schema and include:

- **accounts** – Stable account entity (account_id, email, created_at).
- **events** – Shows/productions (e.g. Wicked, Hamilton): event_id, name, slug.
- **showtimes** – A specific performance of an event: showtime_id, event_id, start_at.
- **orders** – Order header: order_id, account_id, showtime_id, created_at, total_amount.
- **transactions** – Payment records: transaction_id, order_id, amount, occurred_at.
- **pages** – Browsing behavior with **stable** `account_id` and **unstable** `customer_id` (may be merged over time); optional event_id, showtime_id.
- **identity_merges** – Merge log for customer_id (from_customer_id → to_customer_id, merged_at). Use to resolve pages to a canonical identity when building marts.

**Data Quality Notes:** Raw data is intentionally varied (whitespace, casing, amount formats, sentinel values). See [docs/DATA_LIFECYCLE.md](docs/DATA_LIFECYCLE.md) for normalization expectations.

---

## Working with the Stack

### Reset to initial state

If you need to start fresh anytime during the exercise:

```bash
./scripts/reset.sh
```

This will reload initial source data and re-run `dbt seed` and `dbt run`.

### Incremental data ingestion

To append incremental batches from `data/incremental/`:

```bash
bin/ingest                      # Ingest all batches
bin/ingest events/batch_001     # Ingest a specific batch
```

See [docs/DATA_LIFECYCLE.md](docs/DATA_LIFECYCLE.md) for details on data lifecycle, ingestion, and data quality notes.

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

## Command Reference

Run these from the repo root. They wrap `docker compose run --rm ...`.

### dbt Commands

| Command | Purpose |
|--------|---------|
| `bin/dbt run` | Run dbt models |
| `bin/dbt seed` | Load dbt seeds (mappings) |
| `bin/dbt test` | Run dbt tests |
| `bin/dbt build` | Run models and tests |

### Data Management

| Command | Purpose |
|--------|---------|
| `bin/ingest` | Append `data/incremental/*` into source tables |
| `bin/ingest events/batch_001` | Ingest a single batch |
| `bin/load-initial` | Load `data/initial/*` into raw (used by init) |

### PySpark

| Command | Purpose |
|--------|---------|
| `bin/spark-submit example_job.py` | Submit a PySpark job |

---

## Additional Reference

### dbt

- **Sources** in `dbt/models/sources.yml` point at schema `raw`. Reference them with `{{ source('raw', 'table_name') }}`.
- **Models** go in `staging/`, `intermediate/`, and `mart/` under `dbt/models/`. Build from sources (and optionally seeds) in staging, then from staging/intermediate in later layers.
- **Seeds** in `dbt/seeds/` are for mappings only; use `ref()` to reference them.

No local dbt install required; use `bin/dbt run`, `bin/dbt seed`, `bin/dbt test`, etc.

### PySpark

Scripts in `pyspark/` connect to the warehouse via JDBC. Example:

```bash
bin/spark-submit example_job.py
```

**Warehouse connection:** host `warehouse`, port 5432, database `warehouse`, user/password `postgres`/`postgres`. JDBC driver is included; no `--packages` needed. To reduce logs, set `spark.sparkContext.setLogLevel("WARN")` in your script.

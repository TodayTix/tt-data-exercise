# Data lifecycle: init, reset, and incremental ingestion

## Overview

- **Source data** lives in Postgres **source tables** (schema `raw`). It is **not** loaded via dbt seeds.
- **Initial data** is loaded from CSVs in `data/initial/` by `scripts/load_initial_source_data.py` (run as part of init).
- **dbt seeds** are used only for **mappings** (e.g. `event_type_mapping.csv`). Candidates use `ref()` for seeds and `source()` for source tables.
- **Incremental ingestion** appends (or upserts) from `data/incremental/` into the same source tables via `scripts/ingest.py`.

## Initial state

1. Start the warehouse: `docker compose up -d warehouse`
2. Run init: `./scripts/init.sh` (from repo root)

Init will:

- Drop schemas `public` and `raw`, then recreate `public`
- Create schema `raw` and load `data/initial/*.csv` into source tables (e.g. `raw.events`)
- Run `dbt seed` (load mapping seeds into `public`)
- Run `dbt run` (build models from sources and seeds)

## Reset to initial state

From repo root:

```bash
./scripts/reset.sh
```

This runs a full reset (same as init). Optional fast reset via a Postgres dump can be added later.

## Ingesting new data (incremental)

Place CSV batches under `data/incremental/` by entity, e.g.:

- `data/incremental/events/batch_001.csv`
- `data/incremental/events/batch_002.csv`

Then run:

```bash
bin/ingest
```

This appends all CSVs under `data/incremental/` into the corresponding source tables. To run a single batch:

```bash
bin/ingest events/batch_001
```

After ingesting, run dbt to refresh models:

```bash
bin/dbt run
```

## Raw data quality and staging normalization

Raw source data is intentionally varied so that **staging models** are required to normalize before marts. Raw tables may contain:

- **Whitespace** – leading/trailing spaces in text (e.g. event names, emails, page_type).
- **Inconsistent casing** – e.g. `Viewed Product Page` vs `viewed_product_page` vs `VIEWED_PRODUCT_PAGE`.
- **Amount formats** – `150.00`, `$200.50`, or `1,000.00` (with dollar sign or commas) in `orders.total_amount` and `transactions.amount`.
- **Sentinel / null-ish values** – optional FKs may be empty string, `N/A`, or `NULL` instead of SQL NULL.
- **Timestamp consistency** – values are stored as loaded; staging should cast to a single type (e.g. `timestamptz`).

Staging layer responsibilities:

- Trim whitespace on text fields.
- Standardize enums (e.g. `page_type` to a single canonical form such as `lower(snake_case)`).
- Cast amounts to numeric (strip `$` and commas).
- Normalize timestamps to a single type.
- Treat sentinel strings (`N/A`, `NULL`, empty) as SQL NULL for optional fields.

The provided `stg_*` models in `dbt/models/` demonstrate this pattern. Candidates can extend or refine staging so that marts consume clean data. Build marts (e.g. `fct_orders`, `fct_transactions`) from staging models, not directly from raw sources, to support analyses like weekly aggregate sales.

## dbt sources

Source tables are defined in `dbt/models/sources.yml`. Candidates build models from them, e.g.:

```sql
select * from {{ source('raw', 'events') }}
```

## Scripts and shims

| Command / script | Purpose |
|------------------|--------|
| `./scripts/init.sh` | Bring warehouse to initial state (load source data, dbt seed, dbt run) |
| `./scripts/reset.sh` | Full reset (calls init) |
| `bin/dbt run \| seed \| test \| build \| ...` | Run dbt (shim for `docker compose run --rm dbt`) |
| `bin/ingest [batch]` | Append `data/incremental/*` into source tables |
| `bin/load-initial` | Load `data/initial/*` into raw (used by init; rarely run alone) |
| `bin/spark-submit <script.py>` | Submit a PySpark job (e.g. `bin/spark-submit example_job.py`) |

Run shims from the repo root. They wrap the corresponding `docker compose run` commands.

# Data coding exercise

Evaluate the raw source data, identify business metrics a stakeholder would care about, and build dbt models to deliver analysis-ready datasets.

## Overview

For this round, you'll complete a short data engineering exercise using a containerized Postgres warehouse and dbt. The exercise runs locally via Docker. This repository contains source data, a starter dbt project structure, and scripts.

The exercise will be completed **live with the interviewer(s)**. You'll work locally using this repo. You may use your normal tools, including AI assistants.

**This exercise is about reasoning and approach, not finishing everything.** You are not expected to model every table or build a full production-ready warehouse in the time provided. Focus on a sensible subset of the data, clear model structure, and sound assumptions and explanations.

## Getting started

You must have **Docker** installed and be able to run **Docker Compose**. Verify with:

```bash
docker --version
docker compose version
docker run --rm hello-world
docker compose ls
```

Start the stack and load initial data:

```bash
docker compose up -d
./scripts/init.sh
```

Verify dbt works:

```bash
bin/dbt --version
bin/dbt run
```

If these commands complete successfully, you're ready for the interview.

To reset the warehouse to its initial state at any time:

```bash
./scripts/reset.sh
```

## What to build

Explore the source data, consider the relationships and data quality issues, and build dbt models that make the data analysis-ready. The project is set up with three model layers — `staging/` for cleaning raw sources, `intermediate/` for joining and reshaping, and `mart/` for business-ready output.

A large part of this exercise is seeing how you think through the full journey from raw data to business-ready output. We're evaluating your modeling choices, how you handle data quality, what you identify as valuable business insight, and how you structure and test the result. There's no single right answer — show us your approach.

Use documentation to share your rationale, key definitions, assumptions, and any noteworthy challenges you encountered.

## Source data reference

**`dbt/models/sources.yml`** defines the raw source tables and columns. Use it as your starting point. Sources are referenced with `{{ source('raw', 'table_name') }}`.

Raw tables live in the `raw` schema and include:

- **accounts** – Stable account entity (account_id, email, created_at).
- **events** – Shows/productions (e.g. Wicked, Hamilton): event_id, name, slug.
- **showtimes** – A specific performance of an event: showtime_id, event_id, start_at.
- **orders** – Order header: order_id, account_id, showtime_id, created_at, total_amount.
- **transactions** – Payment records: transaction_id, order_id, amount, occurred_at.
- **pages** – Browsing behavior with **stable** `account_id` and **unstable** `customer_id` (may be merged over time); optional event_id, showtime_id.
- **identity_merges** – Merge log for customer_id (from_customer_id → to_customer_id, merged_at). Use to resolve pages to a canonical identity.

## Raw data quality

Raw data is intentionally varied. Expect the following issues:

- **Whitespace** – leading/trailing spaces in text (e.g. event names, emails, page_type).
- **Inconsistent casing** – e.g. `Viewed Product Page` vs `viewed_product_page` vs `VIEWED_PRODUCT_PAGE`.
- **Amount formats** – `150.00`, `$200.50`, or `1,000.00` (with dollar sign or commas) in `orders.total_amount` and `transactions.amount`.
- **Sentinel / null-ish values** – optional FKs may be empty string, `N/A`, or `NULL` instead of SQL NULL.
- **Timestamp consistency** – values are stored as loaded and may need to be cast to a consistent type.

## Project layout

| Path | Purpose |
|------|---------|
| `dbt/models/` | dbt models. `sources.yml` defines raw sources; `staging/`, `intermediate/`, `mart/` are where you add models. |
| `dbt/seeds/` | Mapping seeds (e.g. event_type_mapping.csv); use `ref()` in models. |
| `data/initial/` | CSVs loaded into source tables at init. |
| `data/incremental/` | CSVs appended by `bin/ingest` (e.g. `events/batch_001.csv`). |
| `scripts/` | Init, reset, load_initial_source_data.py, ingest.py. |
| `bin/` | Shims for dbt, ingest, load-initial. |

## Useful commands

Run these from the repo root. They wrap `docker compose run --rm ...`. No local dbt install required.

| Command | Purpose |
|--------|---------|
| `bin/dbt run` | Run dbt models |
| `bin/dbt seed` | Load dbt seeds (mappings) |
| `bin/dbt test` | Run dbt tests |
| `bin/dbt build` | Run models and tests |
| `bin/ingest` | Append `data/incremental/*` into source tables; run `bin/dbt run` after to refresh models |
| `bin/ingest events/batch_001` | Ingest a single batch |
| `bin/load-initial` | Load `data/initial/*` into raw (used by init) |

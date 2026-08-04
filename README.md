# Data platform onboarding exercise: Meridian Live (MARI portfolio company)

Design and build how a newly-acquired MARI portfolio company's ticketing data enters TTG's existing, already-in-production dbt platform — without breaking what's already certified.

## Overview

TTG's data platform (staging → intermediate → mart) is already built and running against TodayTix's own source data. As TTG scales as part of MARI, portfolio companies with their own ticketing systems, their own schemas, and their own data quality quirks need to land in that same platform so stakeholders and AI tooling can query one unified view — not a pile of one-off tables per source.

For this round, you'll take on **Meridian Live**, a fictional (but realistic) MARI portfolio ticketing company, and design + build the onboarding of its data onto the existing platform. The exercise runs locally via Docker, same as the platform's real dev environment. This repository contains the existing platform (already modeled), Meridian's raw source data, and the same scripts/tooling engineers use day to day.

The exercise will be completed **live with the interviewer(s)**. You'll work locally using this repo. You may use your normal tools, including AI assistants.

**This exercise is about architecture and judgment, not finishing everything.** You are not expected to model every Meridian table or handle every edge case in the time provided. We care more about how you reason through the tradeoffs, what you flag as a risk or open question, and how you'd sequence the work than about raw completion. Talk through your thinking as you go — this is as much a design conversation as a coding one.

## What's already built vs. what you'll build

- **Already built (do not need to be modified):** `stg_accounts`, `stg_events`, `stg_showtimes`, `stg_orders`, `stg_transactions`, `stg_pages`, `stg_identity_merges`, `int_pages_identity_resolved`, `dim_customers`, `dim_events`, `fct_transactions`. Run `bin/dbt run` right after `init.sh` and confirm these build cleanly before you touch anything — this is today's certified platform, and downstream dashboards and the AI/agent layer query `fct_transactions` and the dims directly.
- **What you'll build:** the staging → intermediate → mart path for Meridian's raw tables, converging into the existing marts (extending `fct_transactions`/`dim_customers`/`dim_events`, adding new models, or some combination — your call, but be explicit about why) so Meridian's activity shows up alongside TodayTix's in a way stakeholders and downstream consumers can trust.

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

Verify dbt works and the existing platform builds cleanly:

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

Meridian's raw data lands in the same `raw` schema as TTG's (see `dbt/models/sources.yml`), but it isn't a clean drop-in — that's the point. A few problems you'll likely need to reckon with, in no particular order:

- **Identity across systems.** TTG resolves its own unstable `customer_id` via an `identity_merges` log. Meridian has no such log, and there's no shared key between TTG and Meridian besides a loosely-formatted email. Some real people plausibly exist in both systems. How would you approach unifying (or deliberately not unifying) identity here, and what are the failure modes of your approach?
- **Currency.** Meridian orders are priced in GBP, EUR, and SEK, formatted inconsistently (symbols, thousands/decimal separators). `fct_transactions` today assumes USD. How do you normalize without hardcoding rates in SQL?
- **Grain.** TTG's certified fact is one row per transaction (payment). Meridian has no transactions table — its natural grain is the order, and orders can have multiple line items (`meridian_order_items`) at different price points. Do you roll up to order grain to match, introduce something new, or something else? What breaks downstream if you get this wrong?
- **Time.** Meridian stores performance start times as naive local timestamps plus a UTC offset in minutes; TTG's `showtimes.start_at` is already a normalized instant. Reconcile these consistently.
- **Order status and cancellations.** Meridian orders can be `paid`, `refunded`, `partial_refund`, or `cancelled` (casing/whitespace inconsistent). What should "revenue" mean once these exist, and does that change what belongs in a certified fact versus what a stakeholder should query separately?

You don't need to resolve every one of these perfectly. Pick a defensible position on each, implement what you can in the time available, and be ready to explain what you didn't get to and why.

## Source data reference

**`dbt/models/sources.yml`** defines the raw source tables and columns for both TTG and Meridian — use it as your starting point. Sources are referenced with `{{ source('raw', 'table_name') }}`.

TTG tables (already staged/modeled) — see `dbt/models/staging/` for how they're cleaned: `accounts`, `events`, `showtimes`, `orders`, `transactions`, `pages`, `identity_merges`.

Meridian tables (new, unmodeled):

- **meridian_customers** – Meridian's account entity: customer_id, full_name (sometimes blank — guest checkout), email, phone, country, created_at, marketing_opt_in.
- **meridian_venues** – Physical venues Meridian sells for. TTG has no venue entity today.
- **meridian_events** – Shows/productions, each tied to one venue: event_id, title, venue_id, category.
- **meridian_performances** – A specific occurrence of an event: performance_id, event_id, **starts_at_local** (naive, no timezone) + **utc_offset_minutes** (must be combined to get a true instant), doors_at_local.
- **meridian_orders** – Order header, **not** 1:1 with a payment: order_id, customer_id (optional — blank for anonymous/gift), performance_id (optional — blank for non-ticket orders), currency, subtotal, fees, total, status, placed_at.
- **meridian_order_items** – Ticket/merch line items within an order, Meridian's natural grain: order_item_id, order_id, seat_section, unit_price, quantity.
- **meridian_web_sessions** – Browsing behavior: session_id, **cookie_id** (unstable, anonymous), customer_id (only populated once known, e.g. at checkout), event_id (optional), page_type, occurred_at. **There is no identity-resolution table for Meridian** — unlike TTG's `identity_merges`, cookie-to-customer linkage only exists where a session happens to convert.

## Raw data quality

Both sources are intentionally messy, in different ways.

TTG (existing, already handled in staging — for reference):
- Whitespace and inconsistent casing in text fields.
- `$`/comma-formatted amounts in `orders.total_amount` and `transactions.amount`.
- Sentinel nulls (`N/A`, `NULL` string, empty string) on optional FKs in `pages`.

Meridian (new, unhandled):
- **Currency formatting** – `£120.00`, `"95,00 €"` (European decimal comma + symbol), `"1050,00 kr"`, or plain `100.00`, all within the same column.
- **Casing/whitespace** – event categories, order statuses, and page types all vary in casing and padding.
- **Sentinel-ish nulls** – blank, `N/A`, `NULL` string on optional identity/FK fields, same pattern as TTG's `pages` but on different tables.
- **Duplicate identity within Meridian itself** – at least one real person has two `customer_id`s in `meridian_customers` with matching email but slightly different name formatting.
- **No identity_merges equivalent** – see above; this is a real gap, not an oversight to "solve" by inventing data.

## Project layout

| Path | Purpose |
|------|---------|
| `dbt/models/sources.yml` | Defines all raw sources — TTG and Meridian. |
| `dbt/models/staging/` | TTG staging models are already built. Meridian staging models are yours to add. |
| `dbt/models/intermediate/` | TTG has `int_pages_identity_resolved`. Meridian intermediate logic (identity, currency, grain) is yours to add. |
| `dbt/models/mart/` | `dim_customers`, `dim_events`, `fct_transactions` already exist and are certified — extend, don't break. |
| `dbt/seeds/` | `event_type_mapping.csv` (existing) and `fx_rates.csv` (new — currency → USD rate, for candidates to reference rather than hardcode). |
| `data/initial/` | CSVs loaded into source tables at init, both TTG and Meridian. |
| `data/incremental/` | CSVs appended by `bin/ingest` — includes a Meridian batch to test re-runs under new data, same as TTG. |
| `scripts/` | Init, reset, load_initial_source_data.py, ingest.py — all updated to load/ingest Meridian tables. |
| `bin/` | Shims for dbt, ingest, load-initial. |

## Useful commands

Run these from the repo root. They wrap `docker compose run --rm ...`. No local dbt install required.

| Command | Purpose |
|--------|---------|
| `bin/dbt run` | Run dbt models |
| `bin/dbt seed` | Load dbt seeds (mappings, fx rates) |
| `bin/dbt test` | Run dbt tests |
| `bin/dbt build` | Run models and tests |
| `bin/ingest` | Append `data/incremental/*` into source tables; run `bin/dbt run` after to refresh models |
| `bin/ingest meridian_orders/batch_001` | Ingest a single Meridian batch — useful for testing whether your models handle new/incremental Meridian data cleanly |
| `bin/load-initial` | Load `data/initial/*` into raw (used by init) |

# Data platform onboarding exercise: Meridian Live (MARI portfolio company)

Design and build how a newly-acquired MARI portfolio company's ticketing data enters TTG's existing, already-in-production dbt platform — without breaking what's already certified.

## Overview

TTG's data platform (staging → intermediate → mart) is already built and running against TodayTix's own source data. As TTG scales as part of MARI, portfolio companies with their own ticketing systems, their own schemas, and their own data quality quirks need to land in that same platform so stakeholders and AI tooling can query one unified view — not a pile of one-off tables per source.

For this round, you'll take on **Meridian Live**, a fictional (but realistic) MARI portfolio ticketing company, and design + build the onboarding of its data onto the existing platform. The exercise runs locally via Docker, same as the platform's real dev environment. This repository contains the existing platform (already modeled), Meridian's raw source data, and the same scripts/tooling engineers use day to day.

There are two challenges in this repo. **Challenge 1** is onboarding Meridian — modeling work against messy source data. **Challenge 2** is the whitelabel page-tracking pipeline — a pipeline-design problem on code that already works. Your interviewer will tell you which one (or both) you're running.

The exercise will be completed **live with the interviewer(s)**. You'll work locally using this repo. You may use your normal tools, including AI assistants.

**This exercise is about architecture and judgment, not finishing everything.** You are not expected to model every Meridian table or handle every edge case in the time provided. We care more about how you reason through the tradeoffs, what you flag as a risk or open question, and how you'd sequence the work than about raw completion. Talk through your thinking as you go — this is as much a design conversation as a coding one.

## What's already built vs. what you'll build

- **Already built and working:** `stg_accounts`, `stg_events`, `stg_showtimes`, `stg_orders`, `stg_transactions`, `stg_pages`, `stg_identity_merges`, the `base_*__pages` models, `int_pages_identity_resolved`, `dim_customers`, `dim_events`, `fct_transactions`. Run `bin/dbt run` right after `init.sh` and confirm these build cleanly before you touch anything — this is today's certified platform, and downstream dashboards and the AI/agent layer query `fct_transactions` and the dims directly.
- **Challenge 1 builds:** the staging → intermediate → mart path for Meridian's raw tables, converging into the existing marts (extending `fct_transactions`/`dim_customers`/`dim_events`, adding new models, or some combination — your call, but be explicit about why) so Meridian's activity shows up alongside TodayTix's in a way stakeholders and downstream consumers can trust. Nothing in the list above needs to change for this.
- **Challenge 2 changes:** `stg_pages` and the `base_*__pages` models. They build fine today — the problem is what it costs to keep them building. See below.

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

## Challenge 1: onboard Meridian Live

Meridian's raw data lands in the same `raw` schema as TTG's (see `dbt/models/sources.yml`), but it isn't a clean drop-in — that's the point. A few problems you'll likely need to reckon with, in no particular order:

- **Identity across systems.** TTG resolves its own unstable `customer_id` via an `identity_merges` log. Meridian has no such log, and there's no shared key between TTG and Meridian besides a loosely-formatted email. Some real people plausibly exist in both systems. How would you approach unifying (or deliberately not unifying) identity here, and what are the failure modes of your approach?
- **Currency.** Meridian orders are priced in GBP, EUR, and SEK, formatted inconsistently (symbols, thousands/decimal separators). `fct_transactions` today assumes USD. How do you normalize without hardcoding rates in SQL?
- **Grain.** TTG's certified fact is one row per transaction (payment). Meridian has no transactions table — its natural grain is the order, and orders can have multiple line items (`meridian_order_items`) at different price points. Do you roll up to order grain to match, introduce something new, or something else? What breaks downstream if you get this wrong?
- **Time.** Meridian stores performance start times as naive local timestamps plus a UTC offset in minutes; TTG's `showtimes.start_at` is already a normalized instant. Reconcile these consistently.
- **Order status and cancellations.** Meridian orders can be `paid`, `refunded`, `partial_refund`, or `cancelled` (casing/whitespace inconsistent). What should "revenue" mean once these exist, and does that change what belongs in a certified fact versus what a stakeholder should query separately?

You don't need to resolve every one of these perfectly. Pick a defensible position on each, implement what you can in the time available, and be ready to explain what you didn't get to and why.

## Challenge 2: whitelabel storefronts that onboard themselves

TTG powers whitelabel storefronts for other MARI brands. Each brand's web tracking lands in its own schema in the same warehouse, and `stg_pages` unions those brands together with TodayTix's own `raw.pages` so everything downstream sees one stream of page loads.

Three brands follow the tracking standard the team agreed on: the schema is named `wl_<brand>`, the page relation is called `pages`, and the columns come from a fixed vocabulary — `page_id`, `visitor_id`, `account_id`, `page_type`, `occurred_at`, `event_id`, `showtime_id`, `utm_source`, `utm_medium`, `brand_code`. Only `page_id` and `occurred_at` are guaranteed. Which of the rest a brand sends depends on the tracker version it launched with, and some brands send extra columns of their own that mean nothing to us.

One source doesn't follow the standard. `partner_orpheum` was onboarded before the standard existed: unprefixed schema, a `page_views` relation, its own column names, and no page type at all — intent has to be read off the URL path. **It is out of scope.** Leave `base_partner_orpheum__pages` hand-written.

**The problem.** A brand launching is a data-side event: its tracker starts writing into a new schema and nobody on the data team is told. The pipeline doesn't notice. Someone has to declare a source, write a base model, and add a leg to `stg_pages` — a pull request per brand, and brands arrive faster than that queue drains. Until it merges, the brand's rows sit in the warehouse invisible to everyone downstream, and the brand's team is asking why their dashboard is empty.

**Your task.** Change the pipeline so a new standard-conforming brand appears in `stg_pages` with no repository change at all. Mid-session your interviewer will run `bin/add-partner <brand>`, which creates a brand-new `wl_*` schema directly in the warehouse — nothing lands in this repo. You then run `bin/dbt build` against an untouched working tree, and that brand's rows should be there. A list of brand names in a macro is not a solution; it's the same pull request wearing a hat.

Worth having a position on, and worth saying out loud as you go:

- What happens the first time a brand appears with a column set nobody anticipated, at 3am, with no one watching.
- What you give up by taking these sources out of dbt's declared source graph, and whether you're willing to pay it.
- How someone debugging this in six months finds out which brands are actually in the union today.
- Which parts of the union should stay hand-written, and how a reader can tell which is which.
- What should happen if discovery returns nothing at all.

To see what the union currently holds:

```sql
select source_key, count(*) from public.stg_pages group by 1 order by 1;
```

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

Whitelabel schemas (Challenge 2), one per brand, outside `raw`:

- **wl_arcadia.pages** – Sends every column in the standard.
- **wl_northgate.pages** – Older tracker: no `showtime_id`, no `utm_*`. Those columns don't exist on the table.
- **wl_lumen.pages** – The standard set plus `consent_state` and `device_type`, which mean nothing to the platform.
- **partner_orpheum.page_views** – Pre-standard, out of scope: view_id, cookie, member_ref, path, viewed_at, production_ref.
- **wl_sandbox.sessions** – A brand's schema that carries no `pages` relation at all.

`event_id` and `showtime_id` on whitelabel rows are TTG ids — the brands sell TTG inventory through a TTG-powered storefront.

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
| `dbt/models/sources.yml` | Defines all sources — TTG, Meridian, and the declared whitelabel brands. |
| `dbt/models/staging/` | TTG staging models are already built. Meridian staging models are yours to add. |
| `dbt/models/staging/whitelabel/` | One hand-written base model per whitelabel brand, unioned by `stg_pages`. Challenge 2 territory. |
| `dbt/models/intermediate/` | TTG has `int_pages_identity_resolved`. Meridian intermediate logic (identity, currency, grain) is yours to add. |
| `dbt/models/mart/` | `dim_customers`, `dim_events`, `fct_transactions` already exist and are certified — extend, don't break. |
| `dbt/seeds/` | `event_type_mapping.csv` (existing) and `fx_rates.csv` (new — currency → USD rate, for candidates to reference rather than hardcode). |
| `data/initial/` | CSVs loaded into `raw` at init, both TTG and Meridian. |
| `data/whitelabel/` | CSVs loaded into the per-brand schemas at init. `<schema>__<table>.csv` names the relation it becomes. |
| `data/incremental/` | CSVs appended by `bin/ingest` — includes Meridian and whitelabel batches to test re-runs under new data. |
| `scripts/` | Init, reset, load_initial_source_data.py, ingest.py, add_partner.py. |
| `bin/` | Shims for dbt, ingest, load-initial, add-partner. |

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
| `bin/load-initial` | Load `data/initial/*` into raw and `data/whitelabel/*` into the brand schemas (used by init) |
| `bin/add-partner zephyr` | Launch a new whitelabel brand: creates `wl_zephyr.pages` in the warehouse, nothing in the repo. Add `--full` for the complete column set, `--drop` to remove it |

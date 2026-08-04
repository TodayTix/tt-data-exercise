"""
Onboard a new whitelabel brand: create wl_<brand>.pages in the warehouse and fill it
with page-tracking rows. Nothing is written to the repo — this is the warehouse-side
half of onboarding, the half that happens without a pull request.

Run from repo root:
  docker compose run --rm loader python scripts/add_partner.py <brand> [--full] [--rows N]
  docker compose run --rm loader python scripts/add_partner.py <brand> --drop
"""
import argparse
import os
import sys
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import sql

import whitelabel_sources

PGHOST = os.environ.get("PGHOST", "warehouse")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")
PGDATABASE = os.environ.get("PGDATABASE", "warehouse")

# A brand's tracker emits whichever of the standard columns its version knows about.
# The reduced set is what a brand launching on the current tracker sends.
REDUCED_COLUMNS = ["page_id", "visitor_id", "account_id", "page_type", "occurred_at", "event_id", "brand_code"]
FULL_COLUMNS = REDUCED_COLUMNS + ["showtime_id", "utm_source", "utm_medium"]

PAGE_TYPES = ["home", "VIEWED_PRODUCT_PAGE", "viewed_showtime ", "checkout_start", "checkout_complete"]
EVENT_IDS = ["evt_wicked", "evt_hamilton", "evt_lion_king", "evt_six", ""]
SHOWTIME_IDS = ["st_evt_wicked_2", "st_evt_hamilton_21", "st_evt_lion_king_38", "", ""]
UTM = [("google", "cpc"), ("newsletter", "email"), ("", ""), ("facebook", "paid_social"), ("direct", "none")]

FIRST_OCCURRED_AT = datetime(2025, 3, 20, 9, 0, 0)


def build_rows(brand: str, columns: list[str], count: int) -> list[tuple]:
    brand_code = brand[:3].upper()
    rows = []
    for i in range(count):
        occurred_at = FIRST_OCCURRED_AT + timedelta(hours=7 * i)
        utm_source, utm_medium = UTM[i % len(UTM)]
        values = {
            "page_id": f"{brand}_p_{i + 1}",
            "visitor_id": f"vis_{brand}_{i // 2 + 1}",
            "account_id": f"acc_{(i % 20) + 1}" if i % 3 else None,
            "page_type": PAGE_TYPES[i % len(PAGE_TYPES)],
            "occurred_at": occurred_at.isoformat(),
            "event_id": EVENT_IDS[i % len(EVENT_IDS)] or None,
            "brand_code": brand_code,
            "showtime_id": SHOWTIME_IDS[i % len(SHOWTIME_IDS)] or None,
            "utm_source": utm_source or None,
            "utm_medium": utm_medium or None,
        }
        rows.append(tuple(values[c] for c in columns))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Create a whitelabel brand's page-tracking schema.")
    parser.add_argument("brand", help="Brand slug, e.g. zephyr (schema becomes wl_zephyr)")
    parser.add_argument("--full", action="store_true", help="Emit every standard column, not just the reduced set")
    parser.add_argument("--rows", type=int, default=10, help="How many page rows to generate (default 10)")
    parser.add_argument("--drop", action="store_true", help="Drop the brand's schema instead of creating it")
    args = parser.parse_args()

    brand = args.brand.strip().lower().removeprefix("wl_")
    if not brand.replace("_", "").isalnum():
        print(f"Brand slug must be alphanumeric/underscore, got: {args.brand}", file=sys.stderr)
        sys.exit(1)
    schema = f"wl_{brand}"

    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if args.drop:
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
                print(f"Dropped schema {schema}")
            else:
                columns = FULL_COLUMNS if args.full else REDUCED_COLUMNS
                rows = build_rows(brand, columns, args.rows)
                whitelabel_sources.create_relation(cur, schema, "pages", columns)
                n = whitelabel_sources.insert_rows(cur, schema, "pages", columns, rows)
                print(f"Created {schema}.pages ({', '.join(columns)}) with {n} rows")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

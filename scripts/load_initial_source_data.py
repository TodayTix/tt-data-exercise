"""
Create raw schema and source tables, then load CSVs from data/initial/.
Also loads the per-brand whitelabel schemas from data/whitelabel/.
Run from repo root: docker compose run --rm loader python scripts/load_initial_source_data.py
"""
import csv
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

import whitelabel_sources

# Defaults match docker/dbt/profiles.yml and warehouse service
PGHOST = os.environ.get("PGHOST", "warehouse")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")
PGDATABASE = os.environ.get("PGDATABASE", "warehouse")

REPO_ROOT = os.environ.get("REPO_ROOT", "/app")
INIT_DIR = os.path.join(REPO_ROOT, "data", "initial")
WHITELABEL_DIR = os.path.join(REPO_ROOT, "data", "whitelabel")


def _null_if_empty(s):
    """Return None for empty or whitespace-only string, else strip."""
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    return s.strip() if isinstance(s, str) else s


def main():
    if not os.path.isdir(INIT_DIR):
        print(f"Initial data dir not found: {INIT_DIR}", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        dbname=PGDATABASE,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE;")
            cur.execute("CREATE SCHEMA raw;")

            # accounts: account_id, email, created_at
            accounts_csv = os.path.join(INIT_DIR, "accounts.csv")
            if os.path.isfile(accounts_csv):
                cur.execute("""
                    CREATE TABLE raw.accounts (
                        account_id TEXT PRIMARY KEY,
                        email TEXT,
                        created_at TIMESTAMPTZ
                    );
                """)
                with open(accounts_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [(r["account_id"], r["email"], r["created_at"]) for r in reader]
                execute_values(
                    cur,
                    "INSERT INTO raw.accounts (account_id, email, created_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.accounts")

            # events (shows): event_id, name, slug
            events_csv = os.path.join(INIT_DIR, "events.csv")
            if os.path.isfile(events_csv):
                cur.execute("""
                    CREATE TABLE raw.events (
                        event_id TEXT PRIMARY KEY,
                        name TEXT,
                        slug TEXT
                    );
                """)
                with open(events_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [(r["event_id"], r["name"], r["slug"]) for r in reader]
                execute_values(
                    cur,
                    "INSERT INTO raw.events (event_id, name, slug) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.events")

            # showtimes: showtime_id, event_id, start_at
            showtimes_csv = os.path.join(INIT_DIR, "showtimes.csv")
            if os.path.isfile(showtimes_csv):
                cur.execute("""
                    CREATE TABLE raw.showtimes (
                        showtime_id TEXT PRIMARY KEY,
                        event_id TEXT,
                        start_at TIMESTAMPTZ
                    );
                """)
                with open(showtimes_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [(r["showtime_id"], r["event_id"], r["start_at"]) for r in reader]
                execute_values(
                    cur,
                    "INSERT INTO raw.showtimes (showtime_id, event_id, start_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.showtimes")

            # orders: order_id, account_id, showtime_id, created_at, total_amount
            orders_csv = os.path.join(INIT_DIR, "orders.csv")
            if os.path.isfile(orders_csv):
                cur.execute("""
                    CREATE TABLE raw.orders (
                        order_id TEXT PRIMARY KEY,
                        account_id TEXT,
                        showtime_id TEXT,
                        created_at TIMESTAMPTZ,
                        total_amount TEXT
                    );
                """)
                with open(orders_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["order_id"],
                            r["account_id"],
                            _null_if_empty(r.get("showtime_id")),
                            r["created_at"],
                            r["total_amount"],
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.orders (order_id, account_id, showtime_id, created_at, total_amount) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.orders")

            # transactions: transaction_id, order_id, amount, occurred_at
            transactions_csv = os.path.join(INIT_DIR, "transactions.csv")
            if os.path.isfile(transactions_csv):
                cur.execute("""
                    CREATE TABLE raw.transactions (
                        transaction_id TEXT PRIMARY KEY,
                        order_id TEXT,
                        amount TEXT,
                        occurred_at TIMESTAMPTZ
                    );
                """)
                with open(transactions_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (r["transaction_id"], r["order_id"], r["amount"], r["occurred_at"])
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.transactions (transaction_id, order_id, amount, occurred_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.transactions")

            # pages: page_id, account_id, customer_id, page_type, occurred_at, event_id, showtime_id (nullable FKs)
            pages_csv = os.path.join(INIT_DIR, "pages.csv")
            if os.path.isfile(pages_csv):
                cur.execute("""
                    CREATE TABLE raw.pages (
                        page_id TEXT PRIMARY KEY,
                        account_id TEXT,
                        customer_id TEXT,
                        page_type TEXT,
                        occurred_at TIMESTAMPTZ,
                        event_id TEXT,
                        showtime_id TEXT
                    );
                """)
                with open(pages_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["page_id"],
                            _null_if_empty(r.get("account_id")),
                            _null_if_empty(r.get("customer_id")),
                            r["page_type"],
                            r["occurred_at"],
                            _null_if_empty(r.get("event_id")),
                            _null_if_empty(r.get("showtime_id")),
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.pages (page_id, account_id, customer_id, page_type, occurred_at, event_id, showtime_id) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.pages")

            # identity_merges: from_customer_id, to_customer_id, merged_at
            identity_merges_csv = os.path.join(INIT_DIR, "identity_merges.csv")
            if os.path.isfile(identity_merges_csv):
                cur.execute("""
                    CREATE TABLE raw.identity_merges (
                        from_customer_id TEXT,
                        to_customer_id TEXT,
                        merged_at TIMESTAMPTZ
                    );
                """)
                with open(identity_merges_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (r["from_customer_id"], r["to_customer_id"], r["merged_at"])
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.identity_merges (from_customer_id, to_customer_id, merged_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.identity_merges")

            # --- Meridian Live (new MARI portfolio company being onboarded) ---

            # meridian_customers: customer_id, full_name, email, phone, country, created_at, marketing_opt_in
            meridian_customers_csv = os.path.join(INIT_DIR, "meridian_customers.csv")
            if os.path.isfile(meridian_customers_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_customers (
                        customer_id TEXT PRIMARY KEY,
                        full_name TEXT,
                        email TEXT,
                        phone TEXT,
                        country TEXT,
                        created_at TIMESTAMPTZ,
                        marketing_opt_in TEXT
                    );
                """)
                with open(meridian_customers_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["customer_id"],
                            _null_if_empty(r.get("full_name")),
                            r["email"],
                            _null_if_empty(r.get("phone")),
                            r["country"],
                            r["created_at"],
                            _null_if_empty(r.get("marketing_opt_in")),
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_customers (customer_id, full_name, email, phone, country, created_at, marketing_opt_in) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_customers")

            # meridian_venues: venue_id, name, city, country
            meridian_venues_csv = os.path.join(INIT_DIR, "meridian_venues.csv")
            if os.path.isfile(meridian_venues_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_venues (
                        venue_id TEXT PRIMARY KEY,
                        name TEXT,
                        city TEXT,
                        country TEXT
                    );
                """)
                with open(meridian_venues_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [(r["venue_id"], r["name"], r["city"], r["country"]) for r in reader]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_venues (venue_id, name, city, country) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_venues")

            # meridian_events: event_id, title, venue_id, category
            meridian_events_csv = os.path.join(INIT_DIR, "meridian_events.csv")
            if os.path.isfile(meridian_events_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_events (
                        event_id TEXT PRIMARY KEY,
                        title TEXT,
                        venue_id TEXT,
                        category TEXT
                    );
                """)
                with open(meridian_events_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (r["event_id"], r["title"], r["venue_id"], r["category"])
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_events (event_id, title, venue_id, category) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_events")

            # meridian_performances: performance_id, event_id, starts_at_local, utc_offset_minutes, doors_at_local
            meridian_performances_csv = os.path.join(INIT_DIR, "meridian_performances.csv")
            if os.path.isfile(meridian_performances_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_performances (
                        performance_id TEXT PRIMARY KEY,
                        event_id TEXT,
                        starts_at_local TIMESTAMP,
                        utc_offset_minutes INTEGER,
                        doors_at_local TIMESTAMP
                    );
                """)
                with open(meridian_performances_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["performance_id"],
                            r["event_id"],
                            r["starts_at_local"],
                            r["utc_offset_minutes"],
                            r["doors_at_local"],
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_performances (performance_id, event_id, starts_at_local, utc_offset_minutes, doors_at_local) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_performances")

            # meridian_orders: order_id, customer_id, performance_id, currency, subtotal, fees, total, status, placed_at
            meridian_orders_csv = os.path.join(INIT_DIR, "meridian_orders.csv")
            if os.path.isfile(meridian_orders_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_orders (
                        order_id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        performance_id TEXT,
                        currency TEXT,
                        subtotal TEXT,
                        fees TEXT,
                        total TEXT,
                        status TEXT,
                        placed_at TIMESTAMPTZ
                    );
                """)
                with open(meridian_orders_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["order_id"],
                            _null_if_empty(r.get("customer_id")),
                            _null_if_empty(r.get("performance_id")),
                            r["currency"],
                            r["subtotal"],
                            r["fees"],
                            r["total"],
                            r["status"],
                            r["placed_at"],
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_orders (order_id, customer_id, performance_id, currency, subtotal, fees, total, status, placed_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_orders")

            # meridian_order_items: order_item_id, order_id, seat_section, unit_price, quantity
            meridian_order_items_csv = os.path.join(INIT_DIR, "meridian_order_items.csv")
            if os.path.isfile(meridian_order_items_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_order_items (
                        order_item_id TEXT PRIMARY KEY,
                        order_id TEXT,
                        seat_section TEXT,
                        unit_price TEXT,
                        quantity INTEGER
                    );
                """)
                with open(meridian_order_items_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["order_item_id"],
                            r["order_id"],
                            r["seat_section"],
                            r["unit_price"],
                            r["quantity"],
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_order_items (order_item_id, order_id, seat_section, unit_price, quantity) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_order_items")

            # meridian_web_sessions: session_id, cookie_id, customer_id, event_id, page_type, occurred_at
            meridian_web_sessions_csv = os.path.join(INIT_DIR, "meridian_web_sessions.csv")
            if os.path.isfile(meridian_web_sessions_csv):
                cur.execute("""
                    CREATE TABLE raw.meridian_web_sessions (
                        session_id TEXT PRIMARY KEY,
                        cookie_id TEXT,
                        customer_id TEXT,
                        event_id TEXT,
                        page_type TEXT,
                        occurred_at TIMESTAMPTZ
                    );
                """)
                with open(meridian_web_sessions_csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            r["session_id"],
                            r["cookie_id"],
                            _null_if_empty(r.get("customer_id")),
                            _null_if_empty(r.get("event_id")),
                            r["page_type"],
                            r["occurred_at"],
                        )
                        for r in reader
                    ]
                execute_values(
                    cur,
                    "INSERT INTO raw.meridian_web_sessions (session_id, cookie_id, customer_id, event_id, page_type, occurred_at) VALUES %s",
                    rows,
                )
                print(f"Loaded {len(rows)} rows into raw.meridian_web_sessions")

            # --- Whitelabel storefronts: one schema per brand, outside raw ---
            whitelabel_sources.load_dir(cur, WHITELABEL_DIR)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

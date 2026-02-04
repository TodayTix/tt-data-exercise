"""
Create raw schema and source tables, then load CSVs from data/initial/.
Run from repo root: docker compose run --rm loader python scripts/load_initial_source_data.py
"""
import csv
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

# Defaults match docker/dbt/profiles.yml and warehouse service
PGHOST = os.environ.get("PGHOST", "warehouse")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")
PGDATABASE = os.environ.get("PGDATABASE", "warehouse")

REPO_ROOT = os.environ.get("REPO_ROOT", "/app")
INIT_DIR = os.path.join(REPO_ROOT, "data", "initial")


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

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

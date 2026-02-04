"""
Append CSV batches from data/incremental/ into source tables (raw schema).
Run from repo root: docker compose run --rm loader python scripts/ingest.py [batch]
  batch: optional path or name (e.g. pages/batch_001 or pages/batch_001.csv)
         If omitted, processes all CSV files under data/incremental/.
"""
import csv
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

PGHOST = os.environ.get("PGHOST", "warehouse")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGUSER = os.environ.get("PGUSER", "postgres")
PGPASSWORD = os.environ.get("PGPASSWORD", "postgres")
PGDATABASE = os.environ.get("PGDATABASE", "warehouse")

REPO_ROOT = os.environ.get("REPO_ROOT", "/app")
INCREMENTAL_DIR = os.path.join(REPO_ROOT, "data", "incremental")


def _null_if_empty(s):
    """Return None for empty or whitespace-only string, else strip."""
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    return s.strip() if isinstance(s, str) else s


def ingest_accounts(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["account_id"], r["email"], r["created_at"]) for r in reader]
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.accounts (account_id, email, created_at) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_events(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["event_id"], r["name"], r["slug"]) for r in reader]
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.events (event_id, name, slug) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_showtimes(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["showtime_id"], r["event_id"], r["start_at"]) for r in reader]
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.showtimes (showtime_id, event_id, start_at) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_orders(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
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
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.orders (order_id, account_id, showtime_id, created_at, total_amount) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_transactions(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (r["transaction_id"], r["order_id"], r["amount"], r["occurred_at"])
            for r in reader
        ]
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.transactions (transaction_id, order_id, amount, occurred_at) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_pages(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
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
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.pages (page_id, account_id, customer_id, page_type, occurred_at, event_id, showtime_id) VALUES %s",
        rows,
    )
    return len(rows)


def ingest_identity_merges(cur, path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (r["from_customer_id"], r["to_customer_id"], r["merged_at"])
            for r in reader
        ]
    if not rows:
        return 0
    execute_values(
        cur,
        "INSERT INTO raw.identity_merges (from_customer_id, to_customer_id, merged_at) VALUES %s",
        rows,
    )
    return len(rows)


# Map first path segment (entity dir name) to (table_name, ingest_fn)
ENTITY_HANDLERS = {
    "accounts": ("raw.accounts", ingest_accounts),
    "events": ("raw.events", ingest_events),
    "showtimes": ("raw.showtimes", ingest_showtimes),
    "orders": ("raw.orders", ingest_orders),
    "transactions": ("raw.transactions", ingest_transactions),
    "pages": ("raw.pages", ingest_pages),
    "identity_merges": ("raw.identity_merges", ingest_identity_merges),
}


def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if os.path.isfile(arg):
            paths = [arg]
        else:
            candidate = os.path.join(INCREMENTAL_DIR, arg)
            if not candidate.endswith(".csv"):
                candidate = candidate + ".csv"
            if not os.path.isfile(candidate):
                print(f"File not found: {candidate}", file=sys.stderr)
                sys.exit(1)
            paths = [candidate]
    else:
        paths = []
        for root, _dirs, files in os.walk(INCREMENTAL_DIR):
            for f in files:
                if f.endswith(".csv"):
                    paths.append(os.path.join(root, f))
        paths.sort()

    if not paths:
        print("No CSV files under data/incremental/")
        return

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
            for path in paths:
                rel = os.path.relpath(path, INCREMENTAL_DIR)
                parts = rel.split(os.sep)
                entity = parts[0] if parts else None
                if entity not in ENTITY_HANDLERS:
                    print(f"Unknown entity dir: {entity}, skipping {rel}", file=sys.stderr)
                    continue
                table_name, ingest_fn = ENTITY_HANDLERS[entity]
                n = ingest_fn(cur, path)
                print(f"Appended {n} rows to {table_name} from {rel}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

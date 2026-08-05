"""
Load the whitelabel storefront page-tracking schemas.

Each brand's tracking lands in its own schema, one relation per CSV in data/whitelabel/.
A file named <schema>__<table>.csv becomes exactly that relation: wl_arcadia__pages.csv
loads into wl_arcadia.pages. The column set comes from the CSV header, so brands whose
trackers emit different columns land in the warehouse exactly as they really are.

Shared by load_initial_source_data.py and add_partner.py.
"""
import csv
import os

from psycopg2 import sql
from psycopg2.extras import execute_values


def column_type(column: str) -> str:
    """Columns named *_at hold timestamps; the rest land as text and are cleaned in dbt."""
    return "TIMESTAMPTZ" if column.endswith("_at") else "TEXT"


def _null_if_blank(value):
    """Blank cells become NULL. Padding and sentinel text ('N/A', 'NULL') are left alone."""
    if value is None or not value.strip():
        return None
    return value


def create_relation(cur, schema: str, table: str, columns: list[str]) -> None:
    """(Re)create schema.table with one column per name, typed by column_type.

    Only the named relation is replaced; a brand's other relations survive.
    """
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    cur.execute(
        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(schema), sql.Identifier(table)
        )
    )
    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(column_type(c)))
                for c in columns
            ),
        )
    )


def insert_rows(cur, schema: str, table: str, columns: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    execute_values(
        cur,
        sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        ).as_string(cur),
        rows,
    )
    return len(rows)


def load_csv(cur, csv_path: str) -> None:
    """Load one <schema>__<table>.csv into the relation its filename names."""
    stem = os.path.basename(csv_path)[: -len(".csv")]
    schema, _, table = stem.partition("__")
    if not schema or not table:
        raise ValueError(f"Expected <schema>__<table>.csv, got {os.path.basename(csv_path)}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = list(reader.fieldnames or [])
        rows = [tuple(_null_if_blank(r[c]) for c in columns) for r in reader]

    create_relation(cur, schema, table, columns)
    n = insert_rows(cur, schema, table, columns, rows)
    print(f"Loaded {n} rows into {schema}.{table}")


def load_dir(cur, directory: str) -> None:
    if not os.path.isdir(directory):
        return
    for name in sorted(os.listdir(directory)):
        if name.endswith(".csv"):
            load_csv(cur, os.path.join(directory, name))

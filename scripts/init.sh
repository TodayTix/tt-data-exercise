#!/usr/bin/env bash
# Bring warehouse to initial state: drop schemas, load source data, dbt seed, dbt run.
# Run from repo root.

set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting warehouse..."
docker compose up -d warehouse

echo "Waiting for warehouse to be healthy..."
until docker compose exec -T warehouse pg_isready -U postgres; do
  sleep 1
done

echo "Dropping and recreating schemas..."
docker compose exec -T warehouse psql -U postgres -d warehouse -v ON_ERROR_STOP=1 <<'SQL'
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
SQL

echo "Loading initial source data into raw..."
docker compose run --rm loader python scripts/load_initial_source_data.py

echo "Loading dbt seeds (mappings)..."
docker compose run --rm dbt seed

echo "Running dbt models..."
docker compose run --rm dbt run

echo "Initial state ready."

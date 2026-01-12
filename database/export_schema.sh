#!/usr/bin/env sh
# Dump database schema (no data) to database/schema_dump.sql for sharing/import.
# Requires pg_dump available and DB credentials in environment or .env.

set -e
cd "$(dirname "$0")/.."

export PGPASSWORD="${DB_PASSWORD:?set DB_PASSWORD}"
pg_dump \
  --schema-only \
  --no-owner \
  --file="database/schema_dump.sql" \
  --dbname="postgresql://${DB_USER:-postgres}@${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-postgres}"

echo "✅ Schema exported to database/schema_dump.sql"

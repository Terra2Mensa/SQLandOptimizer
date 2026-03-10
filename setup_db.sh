#!/bin/bash
# Creates the cattle_valuation database and initializes the schema.
# Safe to run multiple times — all CREATE statements use IF NOT EXISTS.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Auto-detect PostgreSQL location
for pg_dir in /opt/homebrew/opt/postgresql@*/bin /usr/local/opt/postgresql@*/bin /usr/lib/postgresql/*/bin; do
    [ -d "$pg_dir" ] && export PATH="$pg_dir:$PATH" && break
done

DB_NAME="cattle_valuation"

# Create database if it doesn't exist
if psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Database '$DB_NAME' already exists."
else
    echo "Creating database '$DB_NAME'..."
    createdb "$DB_NAME"
    echo "Database created."
fi

# Initialize schema via Python (uses .env for connection settings)
echo "Initializing schema..."
cd "$SCRIPT_DIR/src"
python3 -c "from db import init_schema; init_schema()"

# Run migration if old tables exist (safe — wrapped in transaction)
if psql "$DB_NAME" -c "SELECT 1 FROM information_schema.tables WHERE table_name='species_valuations'" -tAq 2>/dev/null | grep -q 1; then
    echo "Running migration (merging old valuations tables)..."
    psql "$DB_NAME" -f "$SCRIPT_DIR/sql/migrate_valuations.sql"
else
    echo "No migration needed."
fi

echo "Done. Database is ready."

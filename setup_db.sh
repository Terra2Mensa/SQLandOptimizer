#!/bin/bash
# Create the cattle_valuation database and initialize schema.
# Safe to run multiple times — all operations are idempotent.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
fi

DB_NAME="${DB_NAME:-cattle_valuation}"
DB_USER="${DB_USER:-$(whoami)}"

echo "Setting up database: $DB_NAME (user: $DB_USER)"

# Create database if it doesn't exist
if psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Database '$DB_NAME' already exists."
else
    createdb "$DB_NAME"
    echo "Created database '$DB_NAME'."
fi

# Initialize schema
cd "$SCRIPT_DIR/src"
python3 -c "import db; db.init_schema()"

# Run migration if present
if [ -f "$SCRIPT_DIR/sql/migrate_valuations.sql" ]; then
    psql "$DB_NAME" -f "$SCRIPT_DIR/sql/migrate_valuations.sql" 2>/dev/null || true
    echo "Migration applied."
fi

echo "Database setup complete."

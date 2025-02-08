#!/bin/sh

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

echo "Running PostGIS init script for database: $PGUSER"

psql -d "$PGUSER" -c "CREATE EXTENSION IF NOT EXISTS HSTORE;"
psql -d "$PGUSER" -c "CREATE EXTENSION IF NOT EXISTS POSTGIS;"
psql -d "$PGUSER" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"

echo "Extensions created successfully!"

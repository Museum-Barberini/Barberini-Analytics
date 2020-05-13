#!/bin/bash
# Set up a fresh database and perform a minimal pipeline run on it. Fail if any errors occur.

set -e

cd $(dirname "$0")/../..

echo "Setting up test database ..."
export POSTGRES_DB='barberini_test'
docker exec -i db psql -U postgres -a postgres -v ON_ERROR_STOP=1 <<< "
DROP DATABASE IF EXISTS $POSTGRES_DB;
CREATE DATABASE $POSTGRES_DB;"

echo "Starting luigi container ..."
make startup
trap "make shutdown" EXIT

# Emulating fill_db.sh
echo "Applying all migrations ..."
make docker-do do="POSTGRES_DB=$POSTGRES_DB ./scripts/migrations/migrate.sh"

echo "Running minimal pipeline ..."
make docker-do do="POSTGRES_DB=$POSTGRES_DB make luigi-minimal"

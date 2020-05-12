#!/bin/bash
# Set up a fresh database and perform a minimal pipeline run on it. Fail if any errors occur.

set -e

cd $(dirname "$0")/../..

echo "Setting up test database ..."
set -a
. /etc/barberini-analytics/secrets/database.env
POSTGRES_HOST="localhost"
POSTGRES_DB='barberini_test'
set +a
docker exec -i db psql -U postgres -a postgres <<< "
DROP DATABASE IF EXISTS $POSTGRES_DB;
CREATE DATABASE $POSTGRES_DB;"

echo "Applying all migrations ..."
./scripts/migrations/migrate.sh

echo "Starting luigi container ..."
make startup
trap "make shutdown" EXIT
echo "Running minimal pipeline ..."
make docker-do do="POSTGRES_DB=$POSTGRES_DB make luigi-minimal"

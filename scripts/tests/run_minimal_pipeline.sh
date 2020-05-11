#!/bin/bash
# Set up a fresh database and perform a minimal pipeline run on it. Fail if any errors occur.
set -e

set -a
source /etc/secrets/database.env
POSTGRES_HOST=localhost
POSTGRES_DB='barberini_test'
set +a

set +e
{
    set -e
    docker exec -i db psql -U postgres -a postgres <<< "
        DROP DATABASE IF EXISTS $POSTGRES_DB;
        CREATE DATABASE $POSTGRES_DB;"

    ./scripts/migrations/migrate.sh

    make startup
    do='make luigi-minimal' make docker-do
    make shutdown
}
ERR=$?
rm -r $TEMP_DIR
exit $ERR

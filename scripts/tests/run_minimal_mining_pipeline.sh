#!/bin/bash
# Set up a fresh database and perform a minimal pipeline run on it. Fail if any errors occur.
set -e

export BARBERINI_ANALYTICS_CONTEXT=DEBUG
export OUTPUT_DIR=output-minimal
. ./scripts/tests/gitlab_log.sh


cd "$(dirname "$0")/../.."

start_section setup_database "Setting up test database ..."
    export POSTGRES_DB="barberini_test_$USER"
    docker exec -i barberini_analytics_db \
        psql -U postgres -a postgres -v ON_ERROR_STOP=1 <<< "
    DROP DATABASE IF EXISTS $POSTGRES_DB;
    CREATE DATABASE $POSTGRES_DB;"
end_section setup_database

start_section start_container "Starting luigi container ..."
    make startup
    trap "make docker-cleanup" EXIT
end_section start_container

# Basically, we are emulating fill_db.sh now, just without backups.
start_section apply_migrations "Applying all migrations ..."
    make docker-do do="POSTGRES_DB=$POSTGRES_DB \
		./scripts/migrations/migrate.sh"
end_section apply_migrations

start_section luigi_minimal "Running minimal pipeline ..."
    make docker-do do="POSTGRES_DB=$POSTGRES_DB OUTPUT_DIR=$OUTPUT_DIR \
		make luigi-minimal"
end_section luigi_minimal

start_section check_schema "Checking schema ..."
    make docker-do do="POSTGRES_DB_TEMPLATE=$POSTGRES_DB FULL_TEST=$FULL_TEST \
        make test test=tests/schema/**check*.py"
end_section check_schema

#!/bin/bash
# Set up a fresh database and perform a minimal pipeline run on it. Fail if any errors occur.
set -e

#set -a
#source /etc/secrets/database.env
#set +a

export POSTGRES_DB='barberini_test'
TEMP_DIR="$(mktemp -d minimal_pipeline_XXX)"
export APPLIED_FILE="$TEMP_DIR/applied_migrations.txt"
touch $APPLIED_FILE

set +e
{
    docker exec -i db psql -U postgres -a postgres -c "DROP DATABASE IF EXISTS $POSTGRES_DB;"
    docker exec -i db psql -U postgres -a postgres -c "CREATE DATABASE $POSTGRES_DB;"

    ./scripts/migrations/migrate.sh
    
    make startup
    do='make luigi-minimal' make docker-do
    make shutdown
}
ERR=$?
rm -r $TEMP_DIR
exit $ERR

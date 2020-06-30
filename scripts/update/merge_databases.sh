#!/bin/bash
set -e

if [ -z "$1" ] || [ -z "$2" ]
    then echo "Usage: $0 <FROM-Host> <TO-Host>"
    exit 1
fi

HOST_FROM=$1
HOST_TO=$2

HOST_FROM_DUMP="/tmp/$HOST_FROM.sql"
HOST_TO_DUMP="/tmp/$HOST_TO.sql"
MERGE_DUMP="/tmp/$HOST_FROM-TO-$HOST_TO.pgdump"
export PGUSER="postgres"
export PGDATABASE="barberini"
if [ ! -z $POSTGRES_PASSWORD ]
    then export PGPASSWORD=$POSTGRES_PASSWORD
fi

function enable_foreign_key_checks {
    psql -c "SET session_replication_role = 'origin'" -h "localhost"
}
trap enable_foreign_key_checks EXIT

# Dump data into custom psql format (except exhibition)
echo "Exporting TO-state"
pg_dump -Fc -a -h "$HOST_TO" -f "$HOST_TO_DUMP" -T exhibition -T table_updates
pg_dump -a -h "$HOST_TO" -f "$HOST_TO_DUMP-exhibitions.sql" -t exhibition

# disable search_path modification so existing relations can be found properly
sed -i -e "s/^SELECT pg_catalog.set_config('search_path', '', false);$//" "$HOST_TO_DUMP-exhibitions.sql"

echo "Exporting FROM-state"
pg_dump -a -h "$HOST_FROM" -f "$HOST_FROM_DUMP" --column-inserts --on-conflict-do-nothing -T table_updates -T gomus_daily_entry -T gomus_expected_daily_entry

# Put data into local DB (so it isn't altered on the remote host)
# first put TO-data (data on the host TO which the other should be added)
echo "Re-creating local DB '$PGDATABASE' and applying migrations"
psql -h "localhost" -d "postgres" -c "DROP DATABASE $PGDATABASE;" -c "CREATE DATABASE $PGDATABASE;"
docker exec -it "$USER"-barberini_analytics_luigi bash -c \
    "echo \"\" > /var/lib/postgresql/data/applied_migrations.txt && POSTGRES_DB=\"$PGDATABASE\" make apply-pending-migrations"

echo "Dropping gomus_customer '0' to prevent inserting issues"
psql -h "localhost" -c "DELETE FROM gomus_customer WHERE customer_id = 0;"

echo "Restoring TO-state"
pg_restore -Fc -a -h "localhost" -j 5 --disable-triggers -d $PGDATABASE "$HOST_TO_DUMP"
psql -f "$HOST_TO_DUMP-exhibitions.sql" -h "localhost"

echo "Merging FROM-state"
psql -h "localhost" \
    -c "SET session_replication_role = 'replica';" \
    -f "$HOST_FROM_DUMP" \
    -c "SET session_replication_role = 'origin';"

# ensure performance data is condensed
echo "Condensing performance data"
docker exec -it "$USER"-barberini_analytics_luigi bash -c \
    "POSTGRES_DB=\"$PGDATABASE\" /app/scripts/migrations/migration_024.py"

echo "Exporting MERGED state"
pg_dump -Fc -h "localhost" -f "$MERGE_DUMP"

echo "See '$MERGE_DUMP' for merge result. Insert into empty database like this:"
echo "pg_restore -Fc -h <HOST> -d <DB> -U <USER> $MERGE_DUMP"

#!/bin/bash
set -e

if [ -z "$1" ] || [ -z "$2" ]
    then echo "Usage: $0 <BASE-Host> <PATCH-Host>"
    exit 1
fi

HOST_BASE=$1
HOST_PATCH=$2

HOST_BASE_DUMP="/tmp/$HOST_BASE.sql"
HOST_PATCH_DUMP="/tmp/$HOST_PATCH.sql"
MERGE_DUMP="/tmp/$HOST_BASE-PATCHED_WITH-$HOST_PATCH.pgdump"
export PGUSER="postgres"
export PGDATABASE="barberini"
if [ ! -z $POSTGRES_PASSWORD ]
    then export PGPASSWORD=$POSTGRES_PASSWORD
fi

# Make sure to re-enable foreign key checks locally in case
# something goes wrong
function enable_foreign_key_checks {
    psql -c "SET session_replication_role = 'origin'" -h "localhost"
}
trap enable_foreign_key_checks EXIT

# Dump BASE data into custom psql format (except exhibition)
echo "Exporting BASE-state"
pg_dump -Fc -a -h "$HOST_BASE" -f "$HOST_BASE_DUMP" \
    -T exhibition \  # requires search path modification, see below
    -T table_updates  # database specific logs only
pg_dump -a -h "$HOST_BASE" -f "$HOST_BASE_DUMP-exhibitions.sql" -t exhibition

# disable search_path modification so existing relations can be found properly
sed -i -e "s/^SELECT pg_catalog.set_config('search_path', '', false);$//" \
    "$HOST_BASE_DUMP-exhibitions.sql"

# Dump PATCH data into "INSERT INTO ... ON CONFLICT DO NOTHING";
# SQL statements to be applied to the BASE data
echo "Exporting PATCH-state"
pg_dump -a -h "$HOST_PATCH" -f "$HOST_PATCH_DUMP" \
    --column-inserts --on-conflict-do-nothing \
    -T table_updates \  # database specific logs only
    -T gomus_daily_entry -T gomus_expected_daily_entry \
      # TODO: not necessary at the moment (will be fetched manually)

# Put data into local DB (so it isn't altered on the remote host)
# first put BASE-data, then add PATCH-data
echo "Re-creating local DB '$PGDATABASE' and applying migrations"
psql -h "localhost" -d "postgres" -c "DROP DATABASE $PGDATABASE;" \
    -c "CREATE DATABASE $PGDATABASE;"
docker exec -it "$USER"-barberini_analytics_luigi bash -c \
    "echo \"\" > /var/lib/postgresql/data/applied_migrations.txt \
    && POSTGRES_DB=\"$PGDATABASE\" make apply-pending-migrations"

# Dropping gomus_customer '0' to prevent inserting issues
psql -h "localhost" -c "DELETE PATCH gomus_customer WHERE customer_id = 0;"

echo "Restoring BASE-state"
pg_restore -Fc -a -h "localhost" -j 5 --disable-triggers -d "$PGDATABASE" \
    "$HOST_BASE_DUMP"
psql -f "$HOST_BASE_DUMP-exhibitions.sql" -h "localhost"

echo "Merging PATCH-state"
psql -h "localhost" \
    -c "SET session_replication_role = 'replica';" \
    -f "$HOST_PATCH_DUMP" \
    -c "SET session_replication_role = 'origin';"

# ensure performance data is condensed
echo "Condensing performance data"
docker exec -it "$USER"-barberini_analytics_luigi bash -c \
    "POSTGRES_DB=\"$PGDATABASE\" /app/scripts/migrations/migration_024.py"

echo "Exporting MERGED state"
pg_dump -Fc -h "localhost" -f "$MERGE_DUMP"

echo "See '$MERGE_DUMP' for merge result. Example for importing:"
echo "pg_restore -Fc -h <HOST> -d <DB> -U <USER> $MERGE_DUMP"

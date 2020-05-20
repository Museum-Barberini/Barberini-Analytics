#!/bin/bash
# This script reads files like 'migration_*' from its directory
# and executes all it finds which are not yet listed in the "applied file".
# Expects $POSTGRES_* variables to be available.
# * If $1/$APPLIED_FILE is set, the migrations to be applied will be synced
#   with that file.
#   Otherwise, all migrations will be run and nothing will be recorded.

# Directory and file names
MIGRATION_DIR=$(dirname "$0")
# Globstar guarantees us to get the files in order ascending
MIGRATION_FILES="$MIGRATION_DIR/migration_*"
APPLIED_FILE="$(which "${1:-$APPLIED_FILE}")"
cd "$MIGRATION_DIR/../.."  # provide neutral context for migration scripts

for MIGRATION_FILE in $MIGRATION_FILES
do
    MIGRATION_FILE_NAME="$(basename "$MIGRATION_FILE")"

    if [ ! -z $APPLIED_FILE ] \
        && grep -Fxq "$MIGRATION_FILE_NAME" "$APPLIED_FILE"
    then
        # Migrations has already been applied, skipping to next one
        continue
    fi

    echo "INFO: Applying migration '$MIGRATION_FILE_NAME' ..."
    if [[ "$MIGRATION_FILE_NAME" == *.sql ]]
    then
        # Execute .sql scripts directly

        # Set default Postgres Env variables to
        # avoid having to specify passwords etc. manually
        export PGHOST="$POSTGRES_HOST"
        export PGDATABASE="$POSTGRES_DB"
        export PGUSER="$POSTGRES_USER"
        export PGPASSWORD="$POSTGRES_PASSWORD"

        # ON_ERROR_STOP makes psql abort when the first error is encountered
        # as well as makes it return a non-zero exit code
        psql -q -v ON_ERROR_STOP=1 -f "$MIGRATION_FILE"
    else
        chmod +x "$MIGRATION_FILE"
        # Have the migration interpreted by bash, requires shebang
        "$MIGRATION_FILE"
    fi

    # Check that everything went smoothly
    if [ $? -eq 0 ]
    then
        # Save applied migration
        [ -z "$APPLIED_FILE" ] \
            || (echo "$MIGRATION_FILE_NAME" >> "$APPLIED_FILE")
    else
        # Print warning and exit so that the following migrations
        # are not applied as well
        {
            echo
            echo "ERROR: Migration failed to apply: '$MIGRATION_FILE_NAME'"
            echo "    Please fix the migration script and try it again!"
        } >&2
        exit $EXIT_VAL
    fi
done

echo "INFO: All pending migrations have been applied successfully."

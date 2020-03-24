#!/bin/bash
# This script reads files like 'migration_*' from its directory
# and executes all it finds which are not yet listed in 'applied_migrations'

# Potential ideas for commandline arguments:
# - "reset" to drop database and apply all migrations
# - "test" to set PGDATABASE to "barberini_test"

# Directory and file names
MIGRATION_DIR=$(dirname "$0")

MIGRATION_FILES="$MIGRATION_DIR/migration_*"
APPLIED_FILE="$MIGRATION_DIR/applied_migrations"
DB_CRED_FILE="/etc/secrets/database.env"

for MIGRATION_FILE in $MIGRATION_FILES
do
    MIGRATION_FILE_NAME="$(basename $MIGRATION_FILE)"

    # Only for files which are not yet applied
    if grep -Fxq "$MIGRATION_FILE_NAME" $APPLIED_FILE
    then
        continue
    fi

    if [[ "$MIGRATION_FILE_NAME" == *.sql ]]
    # Execute .sql scripts directly
    then
        # Read in DB credentials
        . $DB_CRED_FILE

        # Set default Postgres Env variables to
        # avoid having to specify passwords etc. manually
        export PGHOST="localhost"
        export PGDATABASE=$POSTGRES_DB
        export PGUSER=$POSTGRES_USER
        export PGPASSWORD=$POSTGRES_PASSWORD

        # ON_ERROR_STOP makes psql abort when the first error is encountered
        # as well as makes it return a non-zero exit code
        psql -v ON_ERROR_STOP=1 < $MIGRATION_FILE
        EXIT_VAL=$?

    # Otherwise let it be interpreted by bash
    # (use shebang-line for scripts!)
    else
        chmod u+x $MIGRATION_FILE
        ./$MIGRATION_FILE
        EXIT_VAL=$?
    fi

    # Check that everything went smoothly
    if [ $EXIT_VAL -eq 0 ]
    then
        # Save applied migration
        echo "$MIGRATION_FILE_NAME" >> $APPLIED_FILE
        echo "INFO: Applied migration: $MIGRATION_FILE_NAME"
    else
        # Print warning and exit so that the following migrations
        # are not applied as well
        {
            echo
            echo "WARNING: Migration failed to apply: $MIGRATION_FILE_NAME"
            echo "    Please resolve the issue manually and add"
            echo "    it to '$(basename $APPLIED_FILE)' or try again!"
        } >&2
        exit $EXIT_VAL
    fi
done

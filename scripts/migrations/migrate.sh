#!/bin/bash
# This script reads files like 'migration_*' from its directory
# and executes all it finds which are not yet listed in 'applied_migrations'

# Directory and file names
MIGRATION_DIR=$(dirname "$0")

MIGRATION_FILES="$MIGRATION_DIR/migration_*"
APPLIED_FILE="$MIGRATION_DIR/applied_migrations"
DB_CRED_FILE="/etc/secrets/database.env"

for MIGRATION_FILE in $MIGRATION_FILES
do
    MIGRATION_FILE="$(basename $MIGRATION_FILE)"

    # Only for files which are not yet applied
    if ! grep -Fxq "$MIGRATION_FILE" $APPLIED_FILE
    then
        # Execute .sql scripts directly
        if [[ "$MIGRATION_FILE" =~ *.sql ]]
        then
            # Read in DB credentials
            . $DB_CRED_FILE
            
            # Set default Postgres Env variables to
            # avoid having to specify passwords etc. manually
            PGHOST="localhost"
            PGDATABASE=$POSTGRES_DB
            PGUSER=$POSTGRES_USER
            PGPASSWORD=$POSTGRES_PASSWORD

            psql < $MIGRATION_FILE
            EXIT_VAL=$?

        # Otherwise let it be interpreted by bash
        # (use shebang-line for python scripts!)
        else
            chmod +x $MIGRATION_FILE
            ./$MIGRATION_FILE
            EXIT_VAL=$?
        fi

        # Check that everything went smoothly
        if [ $EXIT_VAL -eq 0 ]
        then
            # Save applied migration
            echo "$MIGRATION_FILE" >> $APPLIED_FILE
        else
            echo "WARNING: Migration $MIGRATION_FILE failed to apply!"
            echo "Please apply it manually and add it to '$APPLIED_FILE'"
            exit $EXIT_VAL
        fi
    fi
done

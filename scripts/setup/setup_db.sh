#!/bin/bash
# Sets up the database from scratch.
# To be used right after the first 'make startup'.
set -e

cd "$(dirname "$0")"

# shellcheck disable=SC1091
source /etc/barberini-analytics/secrets/database.env
docker exec -it barberini_analytics_db psql -U postgres -a postgres -c "ALTER USER $POSTGRES_USER PASSWORD '$POSTGRES_PASSWORD';"
docker exec -it barberini_analytics_db psql -U postgres -a postgres -c "CREATE DATABASE $POSTGRES_DB;"
./setup_db_config.sh

APPLIED_FILE="/var/barberini-analytics/db-data/applied_migrations.txt"
sudo mkdir "$(dirname "$APPLIED_FILE")"
sudo touch "$APPLIED_FILE"
sudo chmod a+rwx "$APPLIED_FILE"

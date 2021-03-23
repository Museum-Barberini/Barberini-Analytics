#!/bin/bash 
# to be used right after the first 'make startup'
set -e

# shellcheck disable=SC1091
source /etc/barberini-analytics/secrets/database.env
docker exec -it barberini_analytics_db psql -U postgres -a postgres -c "ALTER USER $POSTGRES_USER PASSWORD '$POSTGRES_PASSWORD';"
docker exec -it barberini_analytics_db psql -U postgres -a postgres -c "CREATE DATABASE $POSTGRES_DB;"

APPLIED_FILE="/var/barberini-analytics/db-data/applied_migrations.txt"
sudo mkdir "$(dirname "$APPLIED_FILE")"
sudo touch "$APPLIED_FILE"
sudo chmod a+rwx "$APPLIED_FILE"

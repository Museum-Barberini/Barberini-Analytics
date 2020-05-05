#!/bin/bash 
# to be used right after the first 'make startup'
source /etc/barberini-analytics/secrets/database.env
docker exec -it db psql -U postgres -a postgres -c "ALTER USER $POSTGRES_USER PASSWORD '$POSTGRES_PASSWORD';"
docker exec -it db psql -U postgres -a postgres -c "CREATE DATABASE $POSTGRES_DB;"

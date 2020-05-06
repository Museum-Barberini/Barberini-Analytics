#!/bin/bash
set -e

cd "$(dirname "$0")"
git pull

set -a
. /etc/barberini-analytics/secrets/database.env
POSTGRES_HOST=localhost
./scripts/migrations/migrate.sh /var/barberini-analytics/db-data/applied_migrations.txt
set +a

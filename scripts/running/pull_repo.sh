#!/bin/bash
set -e

cd "$(dirname "$0")"
git pull

# TODO: Test
docker exec -i db /app/scripts/migrations/migrate.sh /var/lib/postgresql/data/applied_migrations.txt

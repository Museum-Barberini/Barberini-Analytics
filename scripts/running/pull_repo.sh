#!/bin/bash
set -e

cd "$(dirname "$0")"
git pull

source /etc/secrets/database.env
../migrations/migrate.sh

#!/usr/bin/env bash
set -e

sudo tee -a /var/barberini-analytics/db-data/pg-data/postgresql.conf <<EOF > /dev/null

include '/etc/postgresql.conf'
EOF

# reload config
sudo make -C /root/barberini-analytics shutdown-db startup-db

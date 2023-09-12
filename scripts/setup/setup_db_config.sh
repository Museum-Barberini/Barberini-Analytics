#!/usr/bin/env bash
set -e

sudo tee -a /var/barberini-analytics/db-data/pg-data/postgresql.conf <<EOF > /dev/null

include '/etc/postgresql.conf'
EOF

# reload config
sudo make -C /root/bp-barberini shutdown-db startup-db

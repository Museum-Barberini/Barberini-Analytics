#!/bin/bash
# This file must be located under '/etc/letsencrypt/renewal-hooks/deploy'
# to enable the TLS certificate to be renewed fully automatically
set -e
DOMAIN=barberini-analytics.westeurope.cloudapp.azure.com
DATA_DIR=/var/barberini-analytics/db-data
SOFTWARE_DIR=/root/bp-barberini

# Copy certificates to folder mounted by DB container
cp /etc/letsencrypt/live/$DOMAIN/fullchain.pem $DATA_DIR/server.crt
cp /etc/letsencrypt/live/$DOMAIN/privkey.pem $DATA_DIR/server.key

# Adjust permissions and ownership of certificates
chown 0:0 $DATA_DIR/server.crt
chown 0:999 $DATA_DIR/server.key

chmod 0644 $DATA_DIR/server.crt
chmod 0640 $DATA_DIR/server.key

# Restart DB container
cd "$SOFTWARE_DIR"
make shutdown-db startup-db

#!/usr/bin/env bash
set -e

DOMAIN=baranalysrv.museum-barberini.de

cd "$(dirname "$0")"

sudo apt-get install certbot
sudo certbot certonly --standalone -d baranalysrv.museum-barberini.de

sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sed "s/\$DOMAIN/$DOMAIN/g" ../maintenance/postgresql-deploy.sh | sudo tee /etc/letsencrypt/renewal-hooks/deploy/postgresql-deploy.sh > /dev/null
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/postgresql-deploy.sh

# test - force renewal
sudo certbot renew --force-renewal

echo '0 0 * * * certbot renew' | sudo crontab -

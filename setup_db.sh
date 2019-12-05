#!/bin/bash
sudo apt install postgresql -y
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'docker';"
sudo -u postgres psql -c "CREATE DATABASE barberini;"
sudo sh -c 'echo "{\"bip\": \"172.16.0.1/16\", \"fixed-cidr\": \"172.16.0.1/17\", \"default-address-pools\": [{\"base\": \"172.17.0.0/16\", \"size\": 24}]}" > /etc/docker/daemon.json'
sudo service docker restart
sudo sh -c 'echo "host all all 172.17.0.0/16 md5" >> /etc/postgresql/$(psql -V | grep -o -m 1 -P "\d\d" | head -n 1)/main/pg_hba.conf'
sudo sh -c 'echo "listen_addresses = '\''*'\''" >> /etc/postgresql/$(psql -V | grep -o -m 1 -P "\d\d" | head -n 1)/main/postgresql.conf'
sudo service postgresql restart

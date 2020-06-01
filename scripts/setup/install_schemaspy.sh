#!/bin/bash
set -e

sudo apt install default-jre
sudo apt-get install graphviz
curl https://jdbc.postgresql.org/download/postgresql-42.2.9.jar --output postgresql.jar
curl -s https://api.github.com/repos/schemaspy/schemaspy/releases/latest \
	| grep "browser_download_url.*jar" \
	| cut -d '"' -f 4 \
	| wget -qi- -O schemaspy.jar

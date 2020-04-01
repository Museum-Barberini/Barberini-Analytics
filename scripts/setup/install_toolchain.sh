#!/bin/bash
set -e
[[ $(id -u) -eq 0 ]] || (echo "Please run as root" ; exit 1)

apt-get update
apt-get install -y make python3-pip

# docker
# May vary depending on your distribution (ubuntu) and architecture (amd64)
# For more information, see https://docs.docker.com/install/linux/docker-ce/ubuntu/
apt-get install -y \
	apt-transport-https \
	ca-certificates \
	curl \
	gnupg2 \
	software-properties-common \
	postgresql-client-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get install -y docker-ce docker-ce-cli containerd.io
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# postgresql
# For some reason, we want to use it outside of our dockers, too.
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
	| sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt -y install postgresql-12 postgresql-client-12


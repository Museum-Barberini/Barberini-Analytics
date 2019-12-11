FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

RUN apt-get update

# install programs to make the container shell more useful
RUN apt-get install -y --fix-missing --no-install-recommends build-essential vim curl gnupg iproute2

# install python
RUN apt-get install -y --no-install-recommends python3.6 python3-pip python3-setuptools python3-dev

# install python packages
RUN pip3 install luigi twitterscraper pandas requests pyyaml xlrd mmh3
#RUN pip3 install Naked

# install psycopg2 (incl. system dependencies)
RUN DEBIAN_FRONTEND=noninteractive \
	apt-get install -y libpq-dev 
RUN pip3 install psycopg2

RUN curl -sL https://deb.nodesource.com/setup_11.x  | bash -
RUN apt-get -y install nodejs

# WORKAROUND until we have multiple Dockers
RUN bash -c "cd ../ && npm i google-trends-api deasync"

# workaround to enable the container to resolve the host "host.docker.internal" to the IP address of the host's bridge
ENTRYPOINT echo $(/sbin/ip route | awk '/default/ { print $3 }') host.docker.internal >> /etc/hosts && /bin/bash

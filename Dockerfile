FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

RUN apt-get update

# install programs to make the container shell more useful
#RUN --fix-missing --no-install-recommends apt-get install -y apt-utils 2>&1 | grep -v "debconf: delaying package configuration, since apt-utils is not installed"
RUN apt-get install -y --fix-missing --no-install-recommends build-essential vim curl gnupg iproute2
RUN apt-get install -y nano

# install python
RUN apt-get install -y --no-install-recommends python3.6 python3-pip python3-setuptools python3-dev

# install python packages
RUN pip3 install wheel luigi
RUN pip3 install twitterscraper pandas requests pyyaml xlrd mmh3 dateparser oauth2client google-api-python-client

# install psycopg2 (incl. system dependencies)
RUN DEBIAN_FRONTEND=noninteractive \
	apt-get install -y libpq-dev 
RUN pip3 install psycopg2

RUN curl -sL https://deb.nodesource.com/setup_11.x  | bash -
RUN apt-get -y install nodejs

# WORKAROUND until we have multiple docker containers
RUN bash -c "cd ../ && npm i google-trends-api deasync"


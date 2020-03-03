FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

RUN apt-get update

# install programs to make the container shell more useful
#RUN apt-get install -y --fix-missing --no-install-recommends apt-utils 2>&1 | grep -v "debconf: delaying package configuration, since apt-utils is not installed"
# Todo: This would be great to fix the apt warnings, but unfortunately, it raises http://security.ubuntu.com/ubuntu 404 Not Found ...
RUN apt-get install -y --fix-missing --no-install-recommends build-essential vim curl gnupg iproute2
RUN apt-get install -y nano psmisc

# install python
RUN apt-get install -y --no-install-recommends python3.6 python3-pip python3-setuptools python3-dev python3-wheel

# install python packages
RUN pip3 install wheel luigi
RUN pip3 install pandas requests pyyaml xlrd bs4 mmh3 dateparser oauth2client xmltodict numpy
RUN pip3 install twitterscraper google-api-python-client

# install psycopg2 (incl. system dependencies)
RUN DEBIAN_FRONTEND=noninteractive \
	apt-get install -y libpq-dev
RUN pip3 install psycopg2

# install node.js
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get -y install nodejs

# install node packages
COPY package*.json ./
RUN npm install
COPY . .

FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

RUN apt-get update
RUN apt-get upgrade -y --no-install-recommends

ARG install='apt-get install -y --no-install-recommends'

# install utilities
#RUN apt-get install -y --fix-missing --no-install-recommends apt-utils 2>&1 | grep -v "debconf: delaying package configuration, since apt-utils is not installed"
# Todo: This would be great to fix the apt warnings, but unfortunately, it raises http://security.ubuntu.com/ubuntu 404 Not Found ...
RUN $install --fix-missing build-essential vim curl gnupg iproute2
RUN $install nano psmisc

# install python
RUN $install python3.6 python3-pip python3-setuptools python3-dev python3-wheel

# install python packages
RUN pip3 install wheel luigi
RUN pip3 install pandas requests pyyaml xlrd bs4 mmh3 dateparser oauth2client xmltodict numpy
RUN pip3 install twitterscraper google-api-python-client

# install psycopg2 (incl. system dependencies)
RUN DEBIAN_FRONTEND=noninteractive $install libpq-dev
RUN pip3 install psycopg2

# install node.js
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN $install nodejs

# install node packages
# Because of serious trouble with volumes and mounting and so, node_modules
# must be installed into the root directory. Other approaches, including manual
# copying of that folder, using [npm install -g], and manipulating the PATH
# variable failed. Don't touch this unless you absolutely know what you do!
COPY package*.json ../
WORKDIR ..
RUN npm install
COPY . /app
WORKDIR /app

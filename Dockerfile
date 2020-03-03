FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

RUN apt-get update
RUN apt-get upgrade -y --no-install-recommends

RUN echo DEBUG $(whoami) $(pwd)
RUN echo DEBUG $(ls)
RUN echo DEBUG dont care about me, im only here to clear the docker cache foo

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
COPY package*.json /node_stuff_tmp/
WORKDIR /node_stuff_tmp
RUN echo DEBUG $(ls)
RUN echo DEBUG $(ls /node_stuff_tmp)
RUN npm install
COPY . /app
RUN echo DEBUG $(ls)
WORKDIR /app
RUN echo DEBUG $(ls)

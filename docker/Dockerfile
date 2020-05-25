FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get upgrade -y --no-install-recommends

ARG INSTALL='apt-get install -y --no-install-recommends'


# Install utilities
RUN $INSTALL apt-utils 2>&1 | grep -v "debconf: delaying package configuration, since apt-utils is not installed"
RUN $INSTALL bash build-essential curl gnupg iproute2 lsb-release wget
RUN $INSTALL git psmisc
# Optional tools for dev experience:
RUN $INSTALL less nano vim

# Install postgresql
RUN wget --quiet --no-check-certificate -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
	| APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1 apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
	| tee /etc/apt/sources.list.d/pgdg.list
# scan new sources
RUN apt-get update
RUN $INSTALL postgresql-client-12

# Install python
RUN $INSTALL python3.6 python3-pip python3-setuptools python3-dev python3-wheel

# Install psycopg2 (incl. system dependencies)
RUN DEBIAN_FRONTEND=noninteractive $INSTALL libpq-dev

# Install python packages
COPY ./requirements.txt /app
RUN pip3 install -r requirements.txt

# Install node.js
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN $INSTALL nodejs

# Install node packages
# Because of serious trouble with volumes and mounting and so, node_modules
# must be installed into the root directory. Other approaches, including manual
# copying of that folder, using [npm install -g], and manipulating the PATH
# variable failed. Don't touch this unless you absolutely know what you do!
WORKDIR /
COPY ./package*.json /
RUN npm install
WORKDIR /app


# Clean up everything
RUN apt-get clean all

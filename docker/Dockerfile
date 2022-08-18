FROM ubuntu:18.04
LABEL last_updated="2022-08-18 18:05:01"
# (The above line needs to be changed after touching any dependency file to
# trigger a rebuild)

WORKDIR /app
VOLUME /app

ENV LANG C.UTF-8

ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update --fix-missing
RUN apt-get upgrade -y --no-install-recommends apt-utils 2>&1 \
  | grep -v 'debconf: .*apt-utils is not installed'

ARG INSTALL='apt-get install -qq --no-install-recommends --fix-missing'


# Install utilities
RUN $INSTALL bash build-essential curl gnupg iproute2 lsb-release wget
RUN $INSTALL git psmisc
# Code analysis tools
RUN $INSTALL shellcheck
# Optional tools for best dev convenience
RUN $INSTALL bash-completion less nano vim

# Install postgresql
RUN wget --quiet --no-check-certificate -O - \
    https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ \
    `lsb_release -cs`-pgdg main" \
  | tee /etc/apt/sources.list.d/pgdg.list
# Scan new sources
RUN apt-get update
RUN $INSTALL postgresql-client-12

# Install python
# NB: We cannot upgrade to python3.7 or something newer at the moment because
# psycopg2 seems to be incompatible.
RUN $INSTALL python3.6 python3-pip python3-setuptools python3-dev \
  python3-wheel
RUN python3 -m pip install --upgrade pip

# Install psycopg2 (incl. system dependencies)
RUN $INSTALL libpq-dev

# Install node.js (required for npm)
RUN curl -sL https://deb.nodesource.com/setup_16.x | bash -
RUN $INSTALL nodejs

# Install node packages
# Because of serious trouble with volumes and mounting and so, node_modules
# must be installed into the root directory. Other approaches, including manual
# copying of that folder, using [npm install -g], and manipulating the PATH
# variable failed. Don't touch this unless you absolutely know what you do!
WORKDIR /
COPY ./package*.json /
RUN npm install
# RUN npm audit fix  # wants to downgrade some remark packages, so we need to
#                      disable it
WORKDIR /app

# Install python packages
COPY requirements.txt /app
RUN python3 -m pip install -r requirements.txt
RUN python3 -m pip check

# Download required resources
## nltk
RUN python3 -c "import nltk; nltk.download('punkt')" 2>&1
## spacy
RUN python3 -m spacy download de_core_news_lg

# Enable bash completion for best dev convenience
# The following command uncomments a section in the bash.bashrc file which is
# responsible for activating the application-specific autocomplete feature of
# bash.
RUN cat /etc/bash.bashrc | stdbuf -o`wc -c < /etc/bash.bashrc` python3 -c \
  'import regex, sys; print(regex.sub(\
    r"(?<=# enable bash completion in interactive shells\n(?:#.*\n)*)#(.*)",\
    r"\1",\
    sys.stdin.read()))' > /etc/bash.bashrc

# Clean up everything
RUN apt-get clean all

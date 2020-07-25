FROM ubuntu:18.04

WORKDIR /app
VOLUME /app

ENV LANG C.UTF-8

ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get upgrade -y --no-install-recommends apt-utils 2>&1 \
  | grep -v 'debconf: .*apt-utils is not installed'

ARG INSTALL='apt-get install -qq --no-install-recommends'


# Install utilities
RUN $INSTALL bash build-essential curl gnupg iproute2 lsb-release wget
RUN $INSTALL git psmisc
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
RUN $INSTALL python3.6 python3-pip python3-setuptools python3-dev \
  python3-wheel

# Install psycopg2 (incl. system dependencies)
RUN $INSTALL libpq-dev

# Install python packages
COPY requirements.txt /app
RUN pip3 install -r requirements.txt
RUN pip3 check

# Install required resources
## nltk
RUN python3 -c "import nltk; nltk.download('punkt')" 2>&1
## spacy
RUN python3 -m spacy download de_core_news_sm

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

# MAKEFILE
# The targets specified in this file are mostly used like aliases.


# ------ Internal variables ------

SHELL := /bin/bash
SSL_CERT_DIR := /var/db-data


# ------ For use outside of containers ------

# --- To manage docker ---

# Start the container luigi. Also start the container db if it is not already running.
# If the container db is being started, start it with ssl encryption if the file '/var/db-data/server.key'.
startup:
	if [[ $$(docker-compose ps --filter status=running --services) != "db" ]]; then\
		if [[ -e $(SSL_CERT_DIR)/server.key ]]; then\
	 		docker-compose -f docker-compose.yml -f docker-compose-enable-ssl.yml up --build -d --no-recreate db;\
		else\
	 		docker-compose -f docker-compose.yml up --build -d --no-recreate db;\
		fi;\
	fi
	# Generate custom hostname for better error logs
	HOSTNAME="$$(hostname)-$$(cat /dev/urandom | tr -dc 'a-z' | fold -w 8 | head -n 1)" \
		docker-compose -p ${USER} up --build -d luigi gplay_api

shutdown:
	docker-compose -p ${USER} rm -sf luigi gplay_api

shutdown-db:
	docker-compose rm -sf db

connect:
	docker-compose -p ${USER} exec luigi bash

# runs a command in the luigi container
# example: sudo make docker-do do='make luigi'
docker-do:
	docker-compose -p ${USER} exec luigi $(do)

docker-clean-cache:
	docker-compose -p ${USER} build --no-cache


# ------ For use inside the Luigi container ------

# --- Control luigi ---

luigi-scheduler:
	luigid --background
	# Waiting for scheduler ...
	bash -c "until echo > /dev/tcp/localhost/8082; do sleep 0.01; done" > /dev/null 2>&1

luigi-restart-scheduler:
	killall luigid
	make luigi-scheduler
	
luigi:
	./scripts/running/fill_db.sh

luigi-task: luigi-scheduler
	mkdir -p output
	luigi --module $(LMODULE) $(LTASK)

luigi-clean:
	rm -rf output

luigi-minimal:
	# the environment variable has to be set to true 
	MINIMAL=True && make luigi

# --- Testing ---

# optional argument: test
# example: make test
# example: make test test=tests/test_twitter.py
test ?= tests/**/test*.py
test: luigi-clean
	mkdir -p output
	# globstar needed to recursively find all .py-files via **
	PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ \
		&& shopt -s globstar \
		&& python3 -m db_test $(test) -v \
		&& make luigi-clean

test-full:
	FULL_TEST=True make test

coverage: luigi-clean
	PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ \
		&& shopt -s globstar \
		&& python3 -m coverage run --source ./src -m db_test -v --failfast --catch tests/**/test*.py -v
	# print coverage results to screen. Parsed by gitlab CI regex to determine MR code coverage.
	python3 -m coverage report
	# generate html report. Is stored as artefact in gitlab CI job (stage: coverage)
	python3 -m coverage html

# --- To access postgres ---

# opens a psql shell inside the database container
db-psql:
	docker exec -it db psql -U postgres -d barberini

# runs a command for the database in the container
# example: sudo make db-do do='\\d'
db = barberini # default database for db-do
db-do:
	docker exec -it db psql -U postgres -a $(db) -c $(do)

db-backup:
	docker exec db pg_dump -U postgres barberini > /var/db-backups/db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql

# Restore the database from a dump/backup
db-restore:
	docker exec -i db psql -U postgres barberini < $(dump)

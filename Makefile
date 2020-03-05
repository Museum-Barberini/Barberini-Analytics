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
	fi;\
	docker-compose -p ${USER} up --build -d luigi

shutdown:
	docker-compose -p ${USER} rm -sf luigi

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
	luigid --background &
	sleep 3 # workaround until scheduler has started

luigi-restart-scheduler:
	sudo killall luigid
	make luigi-scheduler
	
luigi:
	make LMODULE=query_db LTASK=QueryDB luigi-task

luigi-task: luigi-scheduler
	mkdir -p output
	luigi --module $(LMODULE) $(LTASK)

luigi-clean:
	rm -rf output

# --- Testing ---

test: luigi-clean
	mkdir -p output
	# globstar needed to recursively find all .py-files via ** 
	POSTGRES_DB=barberini_test \
		&& shopt -s globstar \
		&& PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ python3 -m unittest tests/**/test*.py -v \
		&& make luigi-clean

coverage: luigi-clean
	POSTGRES_DB=barberini_test && shopt -s globstar && PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ python3 -m coverage run --source ./src -m unittest -v --failfast --catch tests/**/test*.py -v
	# print coverage results to screen. Parsed by gitlab CI regex to determine MR code coverage.
	python3 -m coverage report
	# generate html report. Is stored as artefact in gitlab CI job (stage: coverage)
	python3 -m coverage html

# --- To access postgres ---

# opens a psql shell inside the database container
db-psql:
	docker exec -it db psql -U postgres

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

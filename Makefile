# MAKEFILE
# The targets specified in this file are mostly used like aliases.


# ------ Internal variables ------

SHELL := /bin/bash
SSL_CERT_DIR := /var/barberini-analytics/db-data


# ------ For use outside of containers ------

# --- To manage docker ---

# Start the container luigi. Also start the container db if it is not already running.
# If the container db is being started, start it with ssl encryption if the file '/var/barberini-analytics/db-data/server.key'.
startup: startup-db
	# Generate custom hostname for better error logs
	HOSTNAME="$$(hostname)-$$(cat /dev/urandom | tr -dc 'a-z' | fold -w 8 | head -n 1)" \
		LUIGI_EMAIL_FORMAT=$$( \
		`# Enabled luigi mails iff we are in production context.`; [[ \
			$$BARBERINI_ANALYTICS_CONTEXT = PRODUCTION ]] \
				&& echo "html" || echo "none") \
		docker-compose -p ${USER} up --build -d luigi gplay_api

startup-db:
	if [[ $$(docker-compose ps --filter status=running --services) != "db" ]]; then\
		if [[ -e $(SSL_CERT_DIR)/server.key ]]; then\
	 		docker-compose -f docker-compose.yml -f docker-compose-enable-ssl.yml up --build -d --no-recreate db;\
		else\
	 		docker-compose -f docker-compose.yml up --build -d --no-recreate db;\
		fi;\
	fi

shutdown:
	docker-compose -p ${USER} rm -sf luigi gplay_api

shutdown-db:
	docker-compose rm -sf db

connect:
	docker-compose -p ${USER} exec luigi ${SHELL}

# runs a command in the luigi container
# example: sudo make docker-do do='make luigi'
docker-do:
	docker exec -i "${USER}-luigi" bash -c "$(do)"

docker-clean-cache:
	docker-compose -p ${USER} build --no-cache


# ------ For use inside the Luigi container ------

apply-pending-migrations:
	./scripts/migrations/migrate.sh /var/lib/postgresql/data/applied_migrations.txt

# --- Control luigi ---

luigi-scheduler:
	luigid --background
	# Waiting for scheduler ...
	${SHELL} -c "until echo > /dev/tcp/localhost/8082; do sleep 0.01; done" > /dev/null 2>&1

luigi-restart-scheduler:
	killall luigid
	make luigi-scheduler
	
luigi:
	make luigi-task LMODULE=fill_db LTASK=FillDB

OUTPUT_DIR ?= output # default output directory is 'output'
luigi-task: luigi-scheduler output-folder
	luigi --module $(LMODULE) $(LTASK)

luigi-clean:
	rm -rf $(OUTPUT_DIR)

luigi-minimal: luigi-scheduler luigi-clean output-folder
	MINIMAL=True make luigi

# TODO: Custom output folder per test and minimal?
output-folder:
	mkdir -p $(OUTPUT_DIR)
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
		&& python3 -m coverage run --source ./src -m db_test -v --catch tests/**/test*.py -v
	# print coverage results to screen. Parsed by gitlab CI regex to determine MR code coverage.
	python3 -m coverage report
	# generate html report. Is stored as artifact in gitlab CI job (stage: coverage)
	python3 -m coverage html

# --- To access postgres ---

db = barberini
# default database for db-do
# opens a psql shell inside the database container
db-psql:
	docker exec -it db psql -U postgres -d "$(db)"

# runs a command for the database in the container
# example: sudo make db-do do='\\d'
db-do:
	docker exec -it db psql -U postgres -a "$(db)" -c "$(do)"

db-backup:
	docker exec db pg_dump -U postgres barberini > /var/barberini-analytics/db-backups/db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql

# Restore the database from a dump/backup
db-restore:
	docker exec -i db psql -U postgres barberini < $(dump)

db-schema-report:
	docker exec db pg_dump -U postgres -d barberini -s

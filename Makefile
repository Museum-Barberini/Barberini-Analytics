SHELL := /bin/bash

# variables
TOTALPYPATH := ./src/:./src/_utils/
SSL_CERT_DIR := /var/db-data


# from outside containers

all-the-setup-stuff-for-ci: pull startup connect

pull:
	docker pull ubuntu && docker pull postgres

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

# For use inside the Luigi container

luigi-scheduler:
	luigid --background &
	sleep 3 # workaround until scheduler has started
	
luigi-ui:
	echo WARNING: make target luigi-ui is deprecated: Use luigi-scheduler instead!
	make luigi-scheduler

luigi:
	make LMODULE=query_db LTASK=QueryDB luigi-task

luigi-task: luigi-scheduler
	mkdir -p output
	bash -c "PYTHONPATH=$(TOTALPYPATH) luigi --module $(LMODULE) $(LTASK)"

luigi-clean:
	rm -rf output

psql:
	sudo -u postgres psql

# misc

test: luigi-clean
	mkdir -p output
	# globstar needed to recursively find all .py-files via **
	POSTGRES_DB=barberini_test && shopt -s globstar && PYTHONPATH=$(TOTALPYPATH):./tests/_utils/ python3 -m unittest tests/**/test*.py -v
	make luigi-clean

coverage: luigi-clean
	POSTGRES_DB=barberini_test PYTHONPATH=src:src/_utils:src/gomus:src/gomus/_utils:tests/_utils python3 -m coverage run -m unittest -v --failfast --catch tests/**/test*.py tests/test*.py tests
	python3 -m coverage report
	python3 -m coverage html

# use db-psql to get a psql shell inside the database container
db-psql:
	docker exec -it db psql -U postgres

# use db-do to run a command for the database in the container
# example: sudo make db-do do='\\d'
db = barberini # default database for db-do
db-do:
	docker exec -it db psql -U postgres -a $(db) -c $(do)

db-backup:
	docker exec db pg_dump -U postgres barberini > /var/db-backups/db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql

db-restore:
	docker exec -i db psql -U postgres barberini < $(dump)

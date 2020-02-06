# MAKEFILE
# Awesome Barberini Tool
# This is basically a script folder.


# ------ Internal variables ------

SHELL := /bin/bash
TOTALPYPATH := $(shell find ./src/ -type d | grep -v '/__pycache__' | sed '/\/\./d' | tr '\n' ':' | sed 's/:$$//')
	# this piece of sed-art finds all directories in src (excluding pycache) to build a global namespace


# ------ For use outside of containers ------

# --- To manage docker ---

pull: # TODO: Is this actually required? It worked for me without executing this line.
	docker pull ubuntu && docker pull postgres

startup:
	if [[ $$(docker-compose ps --filter status=running --services) != "db" ]]; then\
		docker-compose up --build -d --no-recreate db;\
	fi;\
	docker-compose -p ${USER} up --build -d luigi

shutdown:
	docker-compose -p ${USER} rm -sf luigi

connect:
	docker-compose -p ${USER} exec luigi bash

# runs a command in the luigi container
# example: sudo make docker-do do='make luigi'
docker-do:
	docker-compose -p ${USER} exec luigi $(do)

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

db-restore:
	docker exec -i db psql -U postgres barberini < $(dump)


# ------ For use inside the Luigi container ------

# --- Control luigi ---

luigi-scheduler:
	luigid --background &
	sleep 3 # workaround until scheduler has started

luigi-restart:
	sudo killall luigid
	make luigi-scheduler
	
luigi:
	make LMODULE=query_db LTASK=QueryDB luigi-task

luigi-task: luigi-scheduler
	mkdir -p output
	bash -c "PYTHONPATH=$(TOTALPYPATH) luigi --module $(LMODULE) $(LTASK)"

luigi-clean:
	rm -rf output

# --- Testing ---

test:
	# globstar needed to recursively find all .py-files via **
	shopt -s globstar && PYTHONPATH=$(TOTALPYPATH) python3 -m unittest tests/**/test*.py -v


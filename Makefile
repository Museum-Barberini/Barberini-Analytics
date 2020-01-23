SHELL := /bin/bash

# variables
TOTALPYPATH := $(shell find ./src/ -type d | grep -v '/__pycache__' | sed '/\/\./d' | tr '\n' ':' | sed 's/:$$//')

# from outside containers

all-the-setup-stuff-for-ci: pull startup connect

pull:
	docker pull ubuntu && docker pull postgres

startup: edcfg
	if [[ $$(docker-compose ps --filter status=running --services) != "db" ]]; then\
	 docker-compose up --build -d --no-recreate db;\
	fi;\
	docker-compose -p ${USER} up --build -d luigi

shutdown:
	docker-compose -p ${USER} rm -sf luigi

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

test:
	mkdir -p output
	PYTHONPATH=$(TOTALPYPATH):./tests/_utils/ python3 -m unittest tests/**/test*.py -v

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


edcfg:
	vim +":set exrc" +q > /dev/null

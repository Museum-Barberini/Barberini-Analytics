SHELL := /bin/bash
# from outside containers

all-the-setup-stuff-for-ci: pull build-luigi startup connect

pull:
	docker pull ubuntu && docker pull postgres

build-luigi:
	docker build -t luigi .

startup:
	# bash -c "if [[ $$(cat /proc/sys/kernel/osrelease) == *Microsoft ]]; then cmd.exe /c docker-compose up --build -d; else docker-compose up --build -d; fi"
	if [[ $$(docker-compose ps --filter status=running --services) == "db" ]]; then\
	 docker-compose up --build -d luigi;\
	else\
	 docker-compose up --build -d;\
	fi

shutdown:
	# docker-compose down && docker-compose rm
	docker-compose rm -sf luigi

connect:
	docker-compose exec luigi bash

# in luigi container use

luigi-ui:
	luigid --background &

luigi:
	make LMODULE=query_db LTASK=QueryDB luigi-task

luigi-task:
	mkdir -p output
	bash -c "PYTHONPATH=$$(find ./src/ -type d | grep -v '/__pycache__' | sed '/\/\./d' | tr '\n' ':' | sed 's/:$$//') luigi --module $(LMODULE) $(LTASK)"

luigi-clean:
	rm -rf output

psql:
	sudo -u postgres psql

# misc

test:
	PYTHONPATH=$$(find ./src/ -type d | grep -v '/__pycache__' | sed '/\/\./d' | tr '\n' ':' | sed 's/:$$//') python3 -m unittest tests/test*.py -v

# use db-psql to get a psql shell inside the database container
db-psql:
	docker exec -it db psql -U postgres

# use db-do to run a command for the database in the container
# example: sudo make db-do do='\\d'
db = barberini # default database for db-do
db-do:
	docker exec -it db psql -U postgres -a $(db) -c $(do)

db-backup:
	docker exec -it db pg_dump -U postgres barberini > /var/db-backups/db_dump_`date +%d-%m-%Y"_"%H_%M_%S`.sql

db-restore:
	cat $(dump) | docker exec -i db psql -U postgres

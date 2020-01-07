# variables

TOTALPYPATH := $(shell find ./src/ -type d | grep -v '/__pycache__' | sed '/\/\./d' | tr '\n' ':' | sed 's/:$$//')
TSTTTLPYPATH := $(TOTALPYPATH):./tests/_utils/

# from outside containers

all-the-setup-stuff-for-ci: pull build-luigi startup connect

pull:
	docker pull ubuntu && docker pull postgres

build-luigi:
	docker build -t luigi .

startup:
	bash -c "if [[ $$(cat /proc/sys/kernel/osrelease) == *Microsoft ]]; then cmd.exe /c docker-compose up --build -d; else docker-compose up --build -d; fi"

shutdown:
	docker-compose down && docker-compose rm

connect:
	docker-compose exec luigi bash

# in luigi container use

luigi-ui:
	luigid --background &

luigi:
	make LMODULE=query_db LTASK=QueryDB luigi-task

luigi-task:
	mkdir -p output
	bash -c "PYTHONPATH=$(TOTALPYPATH) luigi --module $(LMODULE) $(LTASK)"

luigi-clean:
	rm -rf output

psql:
	sudo -u postgres psql

# misc

test:
	mkdir -p output
	jq -rs 'reduce .[] as $$item ({}; . * $$item)' \
		data/barberini-facts.json tests_fake_files/data/barberini-facts.json \
		> tests_fake_files/data/barberini-facts.json
	for test in $$(find tests/ -name *.py) ; do \
		echo "Testing $${test}" ;\
		cp -r tests_fake_files/. . ;\
		PYTHONPATH=$(TSTTTLPYPATH) python3 -m unittest $$test -v ;\
	done

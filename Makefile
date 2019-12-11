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
	#bash -c "PYTHONPATH=\"./src$(find ../src/ -type d < sed '/\/\./d' | tr '\n' ':' | sed 's/:$//')\" luigi --module $(LMODULE) $(LTASK)"
	PYTHONPATH="${PYTHONPATH}:./src:./src/_utils" luigi --module $(LMODULE) $(LTASK)

luigi-clean:
	rm -rf output

psql:
	sudo -u postgres psql

# misc

test:
	PYTHONPATH="./src:./src/_utils" python3 -m unittest tests/test*.py -v

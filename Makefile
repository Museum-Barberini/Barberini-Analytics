# MAKEFILE
# The targets specified in this file are mostly used like aliases.


# ------ Internal variables ------

DOCKER_COMPOSE := docker-compose -f ./docker/docker-compose.yml
PYTHON := python3
SHELL := /bin/bash
SSL_CERT_DIR := /var/barberini-analytics/db-data

# ------ For use outside of containers ------

# --- To manage docker ---

# Start the container luigi. Also start the container barberini_analytics_db if it is not already running.
# If the container barberini_analytics_db is being started, start it with ssl encryption if the file '/var/barberini-analytics/db-data/server.key'.
startup: startup-db
	# Generate custom hostname for better error logs
	HOSTNAME="$$(hostname)-$$(cat /dev/urandom | tr -dc 'a-z' | fold -w 8 | head -n 1)" \
		LUIGI_EMAIL_FORMAT=$$( \
		`# Enabled luigi mails iff we are in production context.`; [[ \
			$$BARBERINI_ANALYTICS_CONTEXT = PRODUCTION ]] \
				&& echo "html" || echo "none") \
		$(DOCKER_COMPOSE) -p ${USER} up --build -d barberini_analytics_luigi barberini_analytics_gplay_api

	echo -e "\e[1m\xf0\x9f\x8f\x84\xe2\x80\x8d`# bash styling magic` To join the" \
		"party, open http://localhost:8082 and run:\n   ssh -L 8082:localhost:$$( \
			docker port ${USER}-barberini_analytics_luigi 8082 | cut -d: -f2 \
		) -fN $$(hostname)\e[0m"

startup-db:
	if [[ $$($(DOCKER_COMPOSE) ps --filter status=running --services) != "barberini_analytics_db" ]]; then\
		if [[ -e $(SSL_CERT_DIR)/server.key ]]; then\
			$(DOCKER_COMPOSE) -f docker/docker-compose-enable-ssl.yml up --build -d --no-recreate barberini_analytics_db;\
		else\
			$(DOCKER_COMPOSE) up --build -d --no-recreate barberini_analytics_db;\
		fi;\
	fi

shutdown:
	$(DOCKER_COMPOSE) -p ${USER} rm -sf barberini_analytics_luigi barberini_analytics_gplay_api

shutdown-db:
	$(DOCKER_COMPOSE) rm -sf barberini_analytics_db

connect:
	$(DOCKER_COMPOSE) -p ${USER} exec barberini_analytics_luigi ${SHELL}

# runs a command in the barberini_analytics_luigi container
# example: sudo make docker-do do='make luigi'
docker-do:
	docker exec -i "${USER}-barberini_analytics_luigi" $(SHELL) -c "$(do)"

# Remove all docker containers
# Keep images for optimization
docker-cleanup: shutdown
	$(SHELL) -c '\
		containers=$$(docker ps -f name=$$USER-* -q);\
		if [ -n "$$containers" ]; then docker rm $$containers; fi;\
	'

# Rebuild docker containers without cache (useful after changing
# requirements.txt has changed, for example)
docker-rebuild:
	$(DOCKER_COMPOSE) -p ${USER} build --no-cache


# ------ For use inside the Luigi container ------

apply-pending-migrations:
	./scripts/migrations/migrate.sh /var/lib/postgresql/data/applied_migrations.txt

EXT ?= sql
create-migration:
	${SHELL} -c "touch $$(find scripts/migrations/ -name 'migration_*' -type f \
		| sed -n 's/\(migration_[[:digit:]]\+\).*$$/\1.$(EXT)/p' \
		| sort -r \
		| head -n 1 \
		| perl -lpe 's/(\d+)/sprintf("%0@{[length($$1)]}d", $$1+1)/e')"

# --- Control luigi ---

luigi-scheduler:
	luigid --background
	# Waiting for scheduler ...
	${SHELL} -c "until echo > /dev/tcp/localhost/8082; do sleep 0.01; done" > /dev/null 2>&1

luigi-restart-scheduler:
	killall luigid || true
	$(MAKE) luigi-scheduler

luigi:
	$(MAKE) luigi-task LMODULE=_fill_db LTASK=FillDb

OUTPUT_DIR ?= output # default output directory is 'output'
luigi-task: luigi-scheduler output-folder
	$(SHELL) -c 'echo luigi "$${LMODULE+--module $$LMODULE}" $(LTASK) $(LARGS)'
	$(SHELL) -c 'luigi $${LMODULE+--module $$LMODULE} $(LTASK) $(LARGS)'

luigi-clean:
	rm -rf $(OUTPUT_DIR)

luigi-minimal: luigi-scheduler luigi-clean output-folder
	MINIMAL=True $(MAKE) luigi

# this target can be used to simply run one task inside the container
luigi-task-in-container: startup
	docker exec $(USER)-barberini_analytics_luigi bash -c \
	'make luigi-task LMODULE=$(LMODULE) LTASK=$(LTASK) LARGS=$(LARGS)'

# TODO: Custom output folder per test and minimal?
output-folder:
	mkdir -p $(OUTPUT_DIR)
# --- Testing ---

# optional argument: test
# example: make test
# example: make test test=tests/test_twitter.py
test ?= tests/**/test*.py
# optional argument: testmodule
# Usually you don't want to change this. All database tests in this solution
# require DatabaseTestSuite from db_test.
test: export OUTPUT_DIR=output_test
test: luigi-clean output-folder
	# globstar needed to recursively find all .py-files via **
	PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ \
		&& shopt -s globstar \
		&& $(PYTHON) -m db_test $(test) -v \
		&& $(MAKE) luigi-clean

test-full:
	FULL_TEST=True $(MAKE) test

coverage: luigi-clean
	PYTHONPATH=$${PYTHONPATH}:./tests/_utils/ \
		&& shopt -s globstar \
		&& $(PYTHON) -m coverage run --source ./src -m db_test -v --catch tests/**/test*.py -v
	# print coverage results to screen. Parsed by gitlab CI regex to determine MR code coverage.
	$(PYTHON) -m coverage report
	# generate html report. Is stored as artifact in gitlab CI job (stage: coverage)
	$(PYTHON) -m coverage html

patch-gomus-version:
	read -p "Please scan the gomus change log for breaking changes. Press Enter to continue"
	$(MAKE) test test=tests.gomus.test_gomus_connection.TestGomusConnection.patch_version
	$(MAKE) test test=tests.gomus.test_gomus_connection.TestGomusConnection.test_version

lint: lint-python lint-markdown

lint-python:
	flake8 -v --show-source .

lint-markdown:
	/node_modules/remark-cli/cli.js -f -u validate-links -u remark-lint-no-dead-urls=$\"skipLocalhost:true, skipUrlPatterns:$\[$\'https://account.google.com$\$\', $\'https://gitlab.com/Museum-Barberini/Barberini-Analytics$\$\']$\" .
	/node_modules/markdownlint-cli/markdownlint.js .

python-bandit:
	shopt -s globstar && bandit -c bandit.yml ./**/*.py

shellcheck:
	shellcheck -s bash $$(find . -name '*.sh')

# --- To access postgres ---

db = barberini
# default database for db-do
# opens a psql shell inside the database container
db-psql:
	docker exec -it barberini_analytics_db psql -U postgres -d "$(db)"

# runs a command for the database in the container
# example: sudo make db-do do='\\d'
db-do:
	docker exec -it barberini_analytics_db psql -U postgres -a "$(db)" -c "$(do)"

db-backup:
	docker exec barberini_analytics_db pg_dump -U postgres barberini \
		> /var/barberini-analytics/db-backups/db_dump_`date +%Y-%m-%d"_"%H_%M_%S`.sql

# Restore the database from a dump/backup
db-restore:
	docker exec -i barberini_analytics_db psql -U postgres barberini < $(dump)

db-schema-report:
	docker exec barberini_analytics_db pg_dump -U postgres -d barberini -s

# --- Maintenance ---

# To be run from within the container
upgrade-requirements:
	pip-upgrade docker/requirements.txt --skip-virtualenv-check -p all
	bash -c 'pip3 check || (echo "⚠ Please define these deps explicitely in requirements.txt" && false)'
	cd docker && ncu -u && npm install
	sed -i '0,/.*/s/\(^# Last updated: \).*/\1'"$(date +"%Y-%m-%d %H:%M:%S")"'/' docker/Dockerfile | less
	echo "Upgrade successful, please run the CI now before merging the" \
		 "upgrades into the master!"

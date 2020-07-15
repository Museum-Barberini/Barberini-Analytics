# Documentation

## Global files

Overview:
- **Private configuration:** `/etc/barberini-analytics/`
  * `secrets/`: [Secret files](#Secrets).
- **Database**: `/var/barberini-analytics/db-data/`
  * `applied_migrations.txt`: Local version registry of the migration system.
  * `pg-data/`: Postgres internals of the database container.
- **Logs:** `/var/log/barberini-analytics/`

  **Remark:** All logs older than two weeks will be cleaned up automatically via `cron.sh`.
  No chance to take a longer vacation!

### Secrets

- `database.env`: Local credentials for the database.
- `keys.env`: Contains access tokens for various public APIs.
- `smtp.env`: Contains access parameters for mailer service.
  The mailer is used to send notification emails about failures in the mining pipeline or CI pipeline.

  If you need to fix the mailer, you should be able to log in on [account.google.com](https://account.google.com) using these credentials.
  Otherwise, just exchange the credentials to use another account.
- `secret_files/`: Various files that should be available in the luigi container.
  Used to store intended amounts of global state.

  Note that from within the luigi container (`make connect`), you can access these files in `/app/secret_files/`!
  * `absa/`: Large external datasets used for the implementation of the bachelor thesis ABSA.
  * `google_gmb_*.json`: Required for the Google Maps task.
    See implementation.

## Workflow

Our recommended consists of:
- a **protected `master`** branch
- a new **merge request** for every change
- a merge policy that rejects every branch unless the **CI has passed** (ideally, use [Pipelines for Merged Results](https://docs.gitlab.com/ee/ci/merge_request_pipelines/pipelines_for_merged_results/))

## Repository overview

- `.gitlab-ci.yml`: Configuration file for [GitLab CI](https://docs.gitlab.com/ee/ci/).
  See [Workflow](#workflow).
- `luigi.cfg`: Configuration file for [Luigi](https://luigi.readthedocs.io/en/stable/index.html), a framework for pipeline orchestration.
  See [the official documentation](https://luigi.readthedocs.io/en/stable/configuration.html).
- `Makefile`: Smorgasbord of every-day commands used during development.
  It is recommended to scan this file once in order to get a brief overview.

  To run any of these targets, for example, type `make connect` to open a connection to the luigi docker container.
  You can also use bash completion for showing all make targets.
- `data/`: Configuration files and constants for several components.
  See [`CONFIGURATION.md`](CONFIGURATION.md).
- `docker/`: Stuff for container configuration.
  See [Docker containers](#docker-containers).
  * `docker-compose*.yml`:
    Configuration files for all docker containers.
  * `Dockerfile`: Scripts to be executed when building the luigi container.
  * `Dockerfile_gplay_api`: Scripts to be executed when building the gplay_api container.
  * `package*.json`: node.js dependencies.
    Necessary for `src/google_trends`.
  * `requirements.txt`: pip dependencies.
  See also: [Update project dependencies](MAINTENANCE.md/#update-project-dependencies).
- (`output*`: Pipeline artifacts.
  Not actually part of this repository.
  Excluded via `.gitignore`.)
- `power_bi/`: Data analytics reports for [Power BI](https://powerbi.microsoft.com/en-us/).

  These reports are stored as [template files](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-templates) to avoid storing secrets in the repository.
- `scripts/`: Smorgasbord of scripts for development, operations, and manual data manipulations.
  * `migrations/`: Migration scripts for separate DB schema changes.
  * `running/`: Scripts for automated pipeline operations.
  * `setup/`: Scripts for setting up the solution on another VM/workstation.

    No claim on completeness.
    See [installation](README.md/#installation).
  * `tests/`: Scripts used as part of CI tests.
    * `run_minimal_mining_pipeline.sh`: Tests setup and running of the entire pipeline in a minimal mode, providing an isolated context against production data.
  * `update/`: Scripts for occasional use.
    See particular docmuentations.
    * `historic_data/`: Scripts to scrape all data from the gomus system again.

      Regularly, we only scrape data of the latest few weeks in the daily pipeline.
      This script can be used if older changes have to be respected (e. g. after the VM has been down for a while, or after any retroactive changes in the booking system have been made that go beyond simple cancellations).
- `src/`: Source code of data integration and analysis tasks.

  The rough structure follows different data sources or analysis fields.  
  In particular, the following paths are of special relevance:

  * `_utils/`: Miscellaneous helper methods and classes used for database access and preprocessing.
  * `gomus/`: Component for integration of the go~mus booking system.
    Museum data are accessed by using webscrapers, undocumented HTTP calls to download reports, and the public API.
    * `_utils/`: Webscrapers.
- `tests/`: Unit tests and acceptance tests for the solution.

  The rough structure follows the `src/` tree.  
  In particular, the following paths are of special relevance:

  * `_utils/`: Classes for our domain-specific test framework
  * `schema/`: Acceptance tests for the database schema
  * `test_data/`: Contains sample files used as mocking inputs or expected outputs of units under test.
- `visualizations/sigma/`: Custom Power BI Visual for the sigma text graph.
  See [documentation there](visualizations/sigma/README.md).

## Migration system
tbd

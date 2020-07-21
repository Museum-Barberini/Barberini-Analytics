# Documentation

## Global files

Overview:
- **Private configuration:** `/etc/barberini-analytics/`
  * `secrets/`: [Secret files](#secrets).
- **Database**: `/var/barberini-analytics/db-data/`
  * `applied_migrations.txt`: Local version registry of the migration system.
    See [Migration system](#migration-system).
  * `server.key`: Let's Encrypt certificate.
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
  * `absa/`: Large external datasets used for the implementation of the bachelor thesis about ABSA.
  * `google_gmb_*.json`: Required for the Google Maps task.
    See implementation.

## Workflow

Our recommended workflow consists of the following policies:
- a **protected `master`** branch
- a new **merge request** for every change
- a merge policy that rejects every branch unless the **CI has passed** (ideally, use [Pipelines for Merged Results](https://docs.gitlab.com/ee/ci/merge_request_pipelines/pipelines_for_merged_results/)).
  For more information about our CI pipeline, head [here](#continuous-integration)

## Repository overview

- `.gitlab-ci.yml`: Configuration file for [GitLab CI](https://docs.gitlab.com/ee/ci/).
  See [Continuous Integration](#continuous-integration).
- `luigi.cfg`: Configuration file for [luigi](https://luigi.readthedocs.io/en/stable/index.html), a framework for pipeline orchestration which is used for our central [mining pipeline](#data-mining-pipeline).

  In particular, you can configure timeouts and notification emails on failures here.
  By the way: If desired, it is also possible to reduce the number of notification mails by [bundling](https://luigi.readthedocs.io/en/stable/configuration.html#batch-notifier) them.

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
  * `requirements.txt`: pip dependencies.
  See also: [Update project dependencies](MAINTENANCE.md#update-project-dependencies).
- (`output*`: Pipeline artifacts.
  Not actually part of this repository.
  Excluded via `.gitignore`.)
- `power_bi/`: Data analytics reports for [Power BI](https://powerbi.microsoft.com/en-us/).

  These reports are stored as [template files](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-templates) to avoid storing secrets in the repository.
- `scripts/`: Smorgasbord of scripts for development, operations, and manual data manipulations.
  * `migrations/`: Migration scripts for separate DB schema changes.
    See [Migration system](#migration-system).
  * `running/`: Scripts for automated pipeline operations.
  * `setup/`: Scripts for setting up the solution on another VM/workstation.

    No claim on completeness.
    See [installation](README.md#installation).
  * `tests/`: Scripts used as part of CI tests.
    * `run_minimal_mining_pipeline.sh`: See the minimal mining description in [CI stages](#continuous-integration).
  * `update/`: Scripts for occasional use.
    See particular docmuentations.
    * `historic_data/`: Scripts to scrape all data from the gomus system again.

      Regularly, we only scrape data of the latest few weeks in the daily pipeline.
      This script can be used if older changes have to be respected (e. g. after the VM has been down for a while, or after any retroactive changes in the booking system have been made that go beyond simple cancellations).
- `src/`: Source code of data integration and analysis tasks.

  The rough structure follows different data sources (see [Data sources](#data-sources)) or analysis fields.  
  In particular, the following paths are of special relevance:

  * `_utils/`: Miscellaneous helper methods and classes used for database access and preprocessing.
  * `gomus/`: Component for the integration of the go~mus booking system.
    Museum data are accessed by using web scrapers, undocumented HTTP calls to download reports, and the public API.
    * `_utils/`: Webscrapers.
- `tests/`: Unit tests and acceptance tests for the solution.

  The rough structure follows the `src/` tree.  
  In particular, the following paths are of special relevance:

  * `_utils/`: Classes for our domain-specific test framework
  * `pbi_reports/`: Crash tests for our Power BI report files.
    See the documentation of the [CI stage](#continuous-integration).
  * `schema/`: Acceptance tests for the database schema
  * `test_data/`: Contains sample files used as mocking inputs or expected outputs of units under test.
- `visualizations/sigma/`: Custom Power BI Visual for the sigma text graph.
  See [documentation there](visualizations/sigma/README.md).

## Docker containers

[Docker](https://www.docker.com/) is a state-of-the-art technology used for containerizing package dependencies.
This project uses multiple dockers for several purposes.
They are all defined from within the `docker/` folder.
At the moment, our solution includes three docker containers:

- `barberini_analytics_luigi` (aka "luigi container" or "the docker"): Primary docker container used for all execution of source code.
  Will be set up and destroyed automatically as part of every automated pipeline run (see `cron.sh`).

  To manually start up the docker, use `make startup`.
  To open a bash shell on the docker, run `make connect`.
  To stop the docker again, use `make shutdown`.

- `barberini_analytics_db` (aka "database container"): Postgres container intended to run permanently.
  If not running, it will be started automatically as part of every automated pipeline run.

- `gplay_api`: Special docker container used to host the Google Play API scraper.
  See `docker/Dockerfile_gplay_api` and `src/gplay/gplay_reviews` for further information.

## Data-mining pipeline

Our whole data-mining pipeline is build using the pipeline orchestration framework [Luigi](https://luigi.readthedocs.io/en/stable/index.html).
It can be configured via the `luigi.cfg` file (see [above](#repository-overview)).
Here is a short crash course about how it works:

- For each script, you can define a *task* by subclassing `luigi.Task`.
- Every task can define three main points of information by overriding the corresponding methods:
  1. **an output target (`output()`):** The name of the file the task will produce.

     A task is defined as `complete()` iff its output file exists.

  2. **dependencies (`requires()`):** A collection of tasks that need to be completed before the requiring task is allowed to run.
  3. **`run()` method:** Contains the actual task logic.
    A task can also yield dynamic dependencies simply by implementing the `run()` as a generator yielding other task instances.

To run our whole pipeline, we define some [`WrapperTask`s](https://luigi.readthedocs.io/en/stable/luigi_patterns.html?highlight=wrappertask#triggering-many-tasks) in the `fill_db` module.
See `make luigi` and `scripts/running/fill_db.sh` to trigger the whole pipeline to run.
To rerun a certain task in development, you need to remove its output file and trigger the pipeline again.

## Data Sources

This project currently collects and integrates data from the following sources:

<div class="tg-wrap"><table id="tg-5z1p4">
<thead>
  <tr>
    <th>Data source</th>
    <th>Relevant data</th>
    <th>Access point</th>
    <th>Authentication</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="5"><b>Gomus</b></td>
    <td>Exhibitions</td>
    <td>
      - Gomus API
    </td>
    <td>None</td>
  </tr>
  <tr>
    <td>Customers</td>
    <td>
      - Excel report<br>
      - HTML scraper
    </td>
    <td rowspan="4">Gomus session ID</td>
  </tr>
  <tr>
    <td>Daily entries</td>
    <td>
      - Excel report
    </td>
  </tr>
  <tr>
    <td>Bookings</td>
    <td>
      - HTML scraper
    </td>
  </tr>
  <tr>
    <td>Orders</td>
    <td>
      <i>tbd</i>
    </td>
  </tr>
  <tr>
    <td><b>Apple App Store</b></td>
    <td>Ratings and comments about the app</td>
    <td>RSS feed</td>
    <td>None</td>
  </tr>
  <tr>
    <td><b>Facebook</b></td>
    <td>
       - Posts by the museum<br>
       - Performance data (likes etc.) for each post<br>
       - Comments on each post
    </td>
    <td>Facebook Graph API</td>
    <td>Facebook Access Token</td>
  </tr>
  <tr>
    <td><b>Google Maps Reviews</b></td>
    <td>Ratings and comments</td>
    <td>Google My Business API</td>
    <td>GMB API Key</td>
  </tr>
  <tr>
    <td><b>Google Play Reviews</b></td>
    <td>Ratings and comments about the app</td>
    <td>Scraper</td>
    <td>None</td>
  </tr>
  <tr>
    <td><b>Instagram</b></td>
    <td>
       - Posts by the museum<br>
       - Performance data (likes etc.) for each post<br>
       - Profile metrics (impressions, clicks, follower count)<br>
       - Audience data (origin, gender)
      </td>
    <td>Facebook Graph API</td>
    <td>Facebook Access Token</td>
  </tr>
  <tr>
    <td><b>Twitter</b></td>
    <td>All tweets related to the museum</td>
    <td>Scraper</td>
    <td>None</td>
  </tr>
</tbody>
</table></div>

## Continuous integration

Our CI pipeline is designed to work for GitLab and it is controlled via the `.gitlab-ci.yml` file.
These are our different CI stages (non-exhaustive list):

- **build:** Test the tech stack set up, especially the Dockerfiles and dependencies.
- **unittest:** Run all unit tests in our `test/` directory.
  See [Repository overview](#repository-overview).
- **minimal-mining-pipeline:** Test setup and running of the entire pipeline in a minimal mode, providing an isolated context against production data.
  See `scripts/test/run_minimal_mining_pipeline.sh`.
- **test-pbi-reports:** Contains crash tests for loading every single `power_bi/` report file in Power BI Desktop.

  Requires a GitLab Runner on Windows to be available.
  See `tests/power_bi`.
- **lint:** Makes sure all Python code in the repository follows the [PEP8 coding style guide](https://www.python.org/dev/peps/pep-0008/).

## Migration system

During the development of this solution, a lot of database schema changes accrued.
To manage the complexity of synchronizing such changes on every VM and every developer's workstation, and in order to ensure internal stability by testing these changes, we developed a schema migration system.

This is how it works: Every schema change is defined as a new migration version.
Pipelines will automatically scan for newly added migration versions and apply them automatically.
All applied migration versions are stored in `/var/barberini-analytics/db-data/applied_migrations.txt`.
Usually you do not want to touch that file manually.

**To add a new migration,** check out the latest version name `nnn` under `scripts/migrations/` and create a new script file named `migration_{nnn + 1}.xxx`.
The script file can have an arbitrary extension, but it must be either an `.sql` transaction, or provide a valid [shebang](https://en.wikipedia.org/wiki/Shebang_(Unix)).
If you use a shebang, make sure to `chmod +x` that file.
You can also run `make migration` to create a new SQL migration script.  
**To apply all pending migrations,** run `make apply-pendining-migrations`.
This is done automatically via `cron.sh`.  
**To apply *all* migrations without respecting the applied-file,** run `scripts/migrations/migrate.sh` without any arguments.

**Remark:** A migration system is characterized by the immutability of all previously defined versions.
This induces the policy that you **never must change any existing migration script** that could already have been applied elsewhere.
**To revert an existing migration script,** create a new migration script in which you implement how to revert the changes of the migration to revert.

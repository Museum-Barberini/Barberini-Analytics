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

  If you need to fix the mailer, you should be able to log in account.google.com using these credentials.
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


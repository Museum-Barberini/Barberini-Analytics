# Configuration

## Barberini facts

The file `data/barberini_facts.jsonc` includes information that is specific to the Museum Barberini.
By modifying it, you can change the social media pages, topics, and items tracked by the Barberini Analytics tools.
You can also reuse this tool for different POIs by changing that file.

The file also includes timespans that are missing in gomus.
These are relevant for visitor prediction.
You can manually add closing timespans and timespans with limited available tickets (like during a pandemic).
WARNING: The latter will need to be adjusted when things are known to turn back to normal to maintain sufficient prediction accuracy.

## Email alerts

Email alerts will be sent if any task in the luigi pipeline fails.
To set up these alerts, edit the `email` section in the `luigi.cfg` file.
To configure the mailing service, edit the `/etc/barberini-analytics/secrets/smtp.env` file and update the following variables:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`

## TLS Encryption

One might want to encrypt their connection to the Database so that sensitive information is not sent in the clear via the Internet.
To do this, we recommend using [LetsEncrypt](https://letsencrypt.org/), since it's free and fairly easy to set up.
To allow for automatic certificate deployment, please check `scripts/setup/postgresql-deploy.sh`.

# Configuration

## Barberini facts

The file `data/barberini_facts.jsonc` includes information that is specific to the Museum Barberini. By modifiying it, you can change the social media pages, topics, and items tracked by Awesome Barberini Tool. You can also reuse this tool for different POIs by changing that file.

### Google Trends

For Google Trends, the facts file contains the value `ids.google.knowledgeId`. It points to the identifier google associates with the museum of interest. This is how you can find out the knowledge id of any another POI:

1. Visit https://google.com/trends (make sure to choose the appropriate TLD for your country)
2. Type in your museum's name without accepting
3. From the suggestion list, don't choose the "search phrase" item itself but the capitalized version of the name that contains the additional information.
4. This is the knowledge graph object. Click it and identify the query parameter that has been added to the URL.

## Email alerts

Email alerts will be sent if any task in the luigi pipeline fails. To set up these alerts, edit the `/etc/barberini-analytics/secrets/smtp.env` file and update the following variables:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`

## TLS Encryption
One might want to encrypt their connection to the Database, so that sensitive information is not sent in the clear via the Internet. To do this, we recommend using [LetsEncrypt](https://letsencrypt.org/), since it's free and fairly easy to set up. To allow for automatic certificate deployment, please check `scripts/setup/postgresql-deploy.sh`.

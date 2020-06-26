# Maintenance


## Symptoms

- `test_gomus_version` failed

  See [Update gomus version](#update-gomus-version).

- `test_session_id_is_valid` failed

  tbd


## Tasks

### Update gomus version

#### Motivation

From time to time, Giant Monkey uses to publish a new version of go~mus. As we are scraping certain contents from the gomus web interface, each of these changes can possibly break our gomus tasks. The `TestGomusVersion` assertions will report any version change.

#### Action

1. Check out the [gomus changelog](https://barberini.gomus.de/wiki/spaces/REL/pages/1787854853) (barberini.gomus.de > Helpdesk > Changelog) and search for possible breaking changes (for example, the layout of the customer data could have changed).
2. Make sure all other gomus tests pass.
3. If there are any breaking changes, the gomus tasks need to be updated.
4. Go to `tests/gomus/test_gomus_version.py` and update the `EXPECTED_VERSION_TAG` constant to match the new version name.

#### Remarks

- In the past, we have experienced a few changes in the gomus HTML format without the version number being incremented. In this case, the `test_gomus_version` scraper needs to be updated. See !169.

import os
import re

import requests

from db_test import DatabaseTestCase, logger

EXPECTED_VERSION_NUMBER = 775
EXPECTED_VERSION_TAG = 'v4.1.5.2 – Premium Edition'
GOMUS_SESS_ID = os.environ['GOMUS_SESS_ID']


class TestGomusConnection(DatabaseTestCase):
    """Tests whether the current gomus connection is valid."""

    def test_valid_session_id(self):
        """Test if GOMUS_SESS_ID env variable contains a valid session ID."""
        response = requests.get(
            'https://barberini.gomus.de/',
            cookies={'_session_id': os.environ['GOMUS_SESS_ID']},
            allow_redirects=False)
        self.assertEqual(200, response.status_code)

    def test_gomus_version(self):

        if GOMUS_SESS_ID == '':
            raise ValueError(
                "Please make sure a valid Gomus session ID is provided")

        response = requests.get(
            'https://barberini.gomus.de/',
            cookies={'_session_id': GOMUS_SESS_ID})
        response.raise_for_status()

        version_hits = {
            ln: match
            for ln, match in [
                (ln, re.match(r'^v\d+\.\d+\.\d+\.\d+ – .+ Edition$', line))
                for ln, line in enumerate(response.text.splitlines())
            ]
            if match
        }

        self.assertTrue(
            version_hits,
            msg="Gomus version number could not be found in HTML string"
        )
        self.assertEqual(
            1,
            len(version_hits),
            msg="Ambiguous version info in gomus HTML"
        )
        ln = list(version_hits.keys())[0]
        version_tag = list(version_hits.values())[0][0]

        self.assertEqual(
            EXPECTED_VERSION_TAG,
            version_tag,
            msg="Gomus version number in HTML string has changed! Please "
                "make sure that all scrapers still work as expected before "
                "updating this test."
        )
        if EXPECTED_VERSION_NUMBER != ln:
            logger.error(
                f"Gomus HTML format has changed (line number of version tag "
                f"has changed from {EXPECTED_VERSION_NUMBER} to {ln}), but "
                f"version number is still the same. Did they push some "
                f"undocumented changes?"
            )

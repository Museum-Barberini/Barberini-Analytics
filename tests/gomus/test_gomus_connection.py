import os
import re

import regex
import requests
from varname import nameof

from db_test import DatabaseTestCase, logger

BASE_URL = 'https://barberini.gomus.de/'
EXPECTED_VERSION_LINE_NUMBER = 770
EXPECTED_VERSION_TAG = 'v4.1.8.22 – Premium Edition'
GOMUS_SESS_ID = os.environ['GOMUS_SESS_ID']


class TestGomusConnection(DatabaseTestCase):
    """Tests whether the current gomus connection is valid."""

    def test_valid_session_id(self):
        """Test if GOMUS_SESS_ID env variable contains a valid session ID."""
        response = requests.get(
            BASE_URL,
            cookies={'_session_id': os.environ['GOMUS_SESS_ID']},
            allow_redirects=False)
        self.assertEqual(200, response.status_code)

    def test_version(self):
        ln, version_tag = self.search_version()

        self.assertEqual(
            EXPECTED_VERSION_TAG,
            version_tag,
            msg="Gomus version number in HTML string has changed! Please "
                "make sure that all scrapers still work as expected before "
                "updating this test."
        )
        if EXPECTED_VERSION_LINE_NUMBER != ln:
            logger.error(
                f"Gomus HTML format has changed (line number of version tag "
                f"has changed from {EXPECTED_VERSION_LINE_NUMBER} to {ln}), "
                f"but version number is still the same. Did they push some "
                f"undocumented changes?"
            )

    def patch_version(self):
        ln, version_tag = self.search_version()

        with open(__file__, 'r') as file:
            source = file.read()

        source = regex.sub(
            rf"(?<=^{nameof(EXPECTED_VERSION_LINE_NUMBER)} = )\d+$",
            str(ln),
            source,
            flags=regex.MULTILINE)
        source = regex.sub(
            rf"(?<=^{nameof(EXPECTED_VERSION_TAG)} = ').+(?='$)",
            version_tag,
            source,
            flags=regex.MULTILINE)

        with open(__file__, 'w') as file:
            file.write(source)

    def search_version(self):
        if GOMUS_SESS_ID == '':
            raise ValueError(
                "Please make sure a valid Gomus session ID is provided")

        response = requests.get(
            BASE_URL,
            cookies={'_session_id': GOMUS_SESS_ID})
        response.raise_for_status()

        version_hits = {
            ln: match
            for ln, match in [
                (ln, re.match(
                    r'^v\d+\.\d+\.\d+(?:\.\d+)? – .+ Edition$',
                    line
                ))
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
        return ln, version_tag

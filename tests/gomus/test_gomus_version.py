#!/usr/bin/env python3
import os

import requests

from db_test import DatabaseTestCase

GOMUS_SESS_ID = os.environ['GOMUS_SESS_ID']
EXPECTED_VERSION_TAG = 'v4.1.4.1 – Premium Edition'


class TestGomusVersion(DatabaseTestCase):

    def test_session_id_is_valid(self):
        """
        test if GOMUS_SESS_ID env variable contains a valid session id
        """

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
            cookies=dict(
                _session_id=GOMUS_SESS_ID))
        if not response.ok:
            response.raise_for_status()

        # currently, the version tag is in this particular line in the HTML
        # if this line no. changes, that also means that adjustments to Gomus
        # have been made
        version_tag = response.text.splitlines()[774]
        self.assertEqual(EXPECTED_VERSION_TAG, version_tag)

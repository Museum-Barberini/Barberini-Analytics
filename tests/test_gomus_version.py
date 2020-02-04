#!/usr/bin/env python3
import os
import requests
import unittest

GOMUS_SESS_ID = os.environ['GOMUS_SESS_ID']
EXPECTED_VERSION_TAG = 'v2.42.48 – Premium Edition'

class TestGomusVersion(unittest.TestCase):
    def test_gomus_version(self):
        if GOMUS_SESS_ID == '':
            print("Please make sure a valid Gomus session ID is provided")
            exit(1)

        response = requests.get('https://barberini.gomus.de/', cookies=dict(_session_id=GOMUS_SESS_ID))
        if not response.status_code == 200:
            print(f"Response was not 200, but {response.status_code} instead")
            exit(1)
    
        # currently, the version tag is in this particular line in the HTML
        # if this line no. changes, that also means that adjustments to Gomus have been made
        version_tag = response.text.splitlines()[762]
        
        self.assertEqual(version_tag, EXPECTED_VERSION_TAG)
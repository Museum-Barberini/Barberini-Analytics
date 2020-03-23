import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from gplay_reviews import FetchGplayReviews
from task_test import DatabaseTaskTest


FAKE_COUNTRY_CODES = ['DE', 'US', 'PL', 'BB']


class TestFetchGplayReviews(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.task = FetchGplayReviews()

    def test_get_language_codes(self):

        language_codes = self.task.get_language_codes()

        self.assertIsInstance(language_codes, list)
        self.assertTrue(all(isinstance(code, str) for code in language_codes))
        self.assertEqual(len(language_codes), 55)

    @patch('apple_appstore.requests.get')
    def test_get_request_returns_bad_status_code(self, mock):
        pass


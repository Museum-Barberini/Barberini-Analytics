import json
import unittest
import pandas as pd
import requests

from unittest.mock import MagicMock, patch
from io import StringIO

from gplay_reviews import FetchGplayReviews
from task_test import DatabaseTaskTest


FAKE_COUNTRY_CODES = ['DE', 'US', 'PL', 'BB']


@patch('gplay_reviews.FetchGplayReviews.get_app_id', 
            return_value='com.barberini.museum.barberinidigital')
class TestFetchGplayReviews(unittest.TestCase):

    def test_get_language_codes(self, _mock_app_id):

        language_codes = FetchGplayReviews().get_language_codes()

        self.assertIsInstance(language_codes, list)
        self.assertTrue(all(isinstance(code, str) for code in language_codes))
        self.assertEqual(len(language_codes), 55)

    def test_fetch_actual_data(self, mock_app_id):

        reviews_en = FetchGplayReviews().fetch_for_language('en')

        self.assertGreater(len(reviews_en), 0)
        keys = ['id', 'date', 'score', 'text', 'title', 'thumbsUp', 'version']
        for review in reviews_en:
            self.assertTrue(all(key in review for key in keys))

    @patch('gplay_reviews.requests.get', text = "abc")
    def test_fetch_for_language_one_return_value(self, mock_get, mock_app_id):

        response_elem = {
                'id': 123,
                'date': '2020-01-04T17:09:33.789Z',
                'score': 4,
                'text': 'text body',
                'title': 'title body',
                'thumbsUp': 0,
                'version': '2.10.7'
            }
        mock_get.return_value.text = json.dumps({'results': [response_elem]})
        mock_get.return_value.status_code = 200

        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 1)
        self.assertDictContainsSubset(response_elem, res[0])

    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_request_failed(self, mock_get, mock_app_id):

        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 400

        self.assertRaises(
            requests.exceptions.HTTPError, 
            FetchGplayReviews().fetch_for_language, "xyz"
        )


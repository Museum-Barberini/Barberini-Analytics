import json
import unittest
import pandas as pd
import requests

from unittest.mock import MagicMock, patch
from io import StringIO

from gplay_reviews import FetchGplayReviews
from task_test import DatabaseTaskTest


response_elem_1 = {
        'id': 123,
        'date': '2020-01-04T17:09:33.789Z',
        'score': 4,
        'text': 'elem 1 text',
        'title': 'elem 2 title',
        'thumbsUp': 0,
        'version': '2.10.7'
}
response_elem_2 = {
        'id': "abc",
        'date': '2019-02-24T09:20:00.123Z',
        'score': 0,
        'text': 'elem 2 text',
        'title': 'elem 2 title',
        'thumbsUp': 9,
        'version': '1.1.2'
}

@patch('gplay_reviews.FetchGplayReviews.get_app_id', 
            return_value='com.barberini.museum.barberinidigital')
class TestFetchGplayReviews(unittest.TestCase):

    def test_get_language_codes(self, _mock_app_id):

        language_codes = FetchGplayReviews().get_language_codes()

        self.assertIsInstance(language_codes, list)
        self.assertTrue(all(isinstance(code, str) and len(code) <= 5 for code in language_codes))
        self.assertEqual(len(language_codes), 55)

    def test_fetch_actual_data(self, mock_app_id):

        reviews_en = FetchGplayReviews().fetch_for_language('en')

        self.assertGreater(len(reviews_en), 0)
        keys = ['id', 'date', 'score', 'text', 'title', 'thumbsUp', 'version']
        for review in reviews_en:
            self.assertTrue(all(key in review for key in keys))
    
    def test_fetch_actual_data_wrong_country_code(self, mock_app_id):

        reviews = FetchGplayReviews().fetch_for_language('nonexisting language')
        reviews_en = FetchGplayReviews().fetch_for_language('en')

        # If the supplied language code does not exist, english reviews are returned.
        # This test ensures that we notice if the behaviour changes.
        self.assertEqual(reviews, reviews_en)

    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_one_return_value(self, mock_get, mock_app_id):

        mock_get.return_value.text = json.dumps({'results': [response_elem_1]})
        
        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 1)
        self.assertDictContainsSubset(response_elem_1, res[0])
    
    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_multiple_return_values(self, mock_get, mock_app_id):

        mock_get.return_value.text = json.dumps({'results': [response_elem_1, response_elem_2]})
        
        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 2)
        self.assertDictContainsSubset(response_elem_1, res[0])
        self.assertDictContainsSubset(response_elem_2, res[1])
    
    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_no_reviews_returned(self, mock_get, mock_app_id):

        mock_get.return_value.text = json.dumps({'results': []})
        
        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertIsInstance(res, list)
        self.assertEqual(len(res), 0)

    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_request_failed(self, mock_get, mock_app_id):

        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 400

        self.assertRaises(
            requests.exceptions.HTTPError, 
            FetchGplayReviews().fetch_for_language, "xyz"
        )

    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
            side_effect=[[response_elem_1], [response_elem_2], []])
    @patch('gplay_reviews.FetchGplayReviews.get_language_codes',
            return_value=['en', 'de', 'fr'])
    def test_fetch_all_multiple_return_values(self, mock_lang, mock_fetch, mock_app_id):

        res = FetchGplayReviews().fetch_all()

        self.assertIsInstance(res, pd.DataFrame)
        pd.testing.assert_frame_equal(res, pd.DataFrame([response_elem_1, response_elem_2]))

    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
            return_value=[])
    def test_fetch_all_language_results_are_empty(self, mock_fetch, mock_app_id):

        res = FetchGplayReviews().fetch_all()

        pd.testing.assert_frame_equal(res, pd.DataFrame())

    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
            side_effect=[[response_elem_1, response_elem_2, response_elem_2], [response_elem_1]])
    @patch('gplay_reviews.FetchGplayReviews.get_language_codes',
            return_value=['en', 'de'])
    def test_fetch_all_drops_duplicates(self, mock_lang, mock_fetch, mock_app_id):
        
        res = FetchGplayReviews().fetch_all()

        pd.testing.assert_frame_equal(res, pd.DataFrame([response_elem_1, response_elem_2]))



import json
import pandas as pd
import requests
import unittest

from luigi.format import UTF8
from luigi.mock import MockTarget
from unittest.mock import patch

from gplay_reviews import FetchGplayReviews


response_elem_1 = {
    'id': '123a',
    'date': '2020-01-04T17:09:33.789Z',
    'score': 4,
    'text': 'elem 1 text üÄö',
    'title': 'elem 2 title',
    'thumbsUp': 0,
    'version': '2.10.7'
}
response_elem_1_renamed_cols = {
    'playstore_review_id': '123a',
    'text': 'elem 1 text üÄö',
    'rating': 4,
    'app_version': '2.10.7',
    'thumbs_up': 0,
    'title': 'elem 2 title',
    'date': '2020-01-04T17:09:33.789Z'
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


class TestFetchGplayReviews(unittest.TestCase):

    @patch('gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de'])
    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[[response_elem_1], [response_elem_1], []])
    @patch.object(FetchGplayReviews, 'output')
    @patch.object(FetchGplayReviews, 'input')
    def test_run(self, input_mock, output_mock, mock_fetch, mock_lang):

        input_target = MockTarget('museum_facts', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as fp:
            json.dump(
                {'ids': {
                    'gplay': {
                        'appId': 'com.barberini.museum.barberinidigital'}}},
                fp
            )
        output_target = MockTarget('gplay_reviews', format=UTF8)
        output_mock.return_value = output_target

        FetchGplayReviews().run()

        expected = pd.DataFrame([response_elem_1_renamed_cols])
        with output_target.open('r') as output_file:
            actual = pd.read_csv(output_file)

        pd.testing.assert_frame_equal(expected, actual)

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    def test_fetch_language_actual_data(self, mock_app_id):

        reviews_en = FetchGplayReviews().fetch_for_language('en')

        self.assertTrue(reviews_en)
        keys = ['id', 'date', 'score', 'text', 'title', 'thumbsUp', 'version']
        for review in reviews_en:
            self.assertCountEqual(keys, review.keys())

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    def test_fetch_language_actual_data_wrong_country_code(self, mock_app_id):

        reviews = FetchGplayReviews().fetch_for_language('nonexisting lang')
        reviews_en = FetchGplayReviews().fetch_for_language('en')

        # If the supplied language code does not exist, english reviews
        # are returned.This test ensures that we notice if the
        # behaviour changes.
        self.assertEqual(reviews, reviews_en)

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[[response_elem_1], [response_elem_2], []])
    @patch('gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de', 'fr'])
    def test_fetch_all_multiple_return_values(self, mock_lang,
                                              mock_fetch, mock_app_id):

        res = FetchGplayReviews().fetch_all()

        self.assertIsInstance(res, pd.DataFrame)
        pd.testing.assert_frame_equal(
            res, pd.DataFrame([response_elem_1, response_elem_2]))

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
           return_value=[])
    def test_fetch_lang_all_results_are_empty(self, mock_fetch, mock_app_id):

        res = FetchGplayReviews().fetch_all()

        pd.testing.assert_frame_equal(
            res,
            pd.DataFrame(columns=['id', 'date', 'score', 'text',
                                  'title', 'thumbsUp', 'version'])
        )

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[[response_elem_1, response_elem_2,
                         response_elem_2], [response_elem_1]])
    @patch('gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de'])
    def test_fetch_all_no_duplicates(self, mock_lang, mock_fetch, mock_app_id):

        res = FetchGplayReviews().fetch_all()

        pd.testing.assert_frame_equal(
            res, pd.DataFrame([response_elem_1, response_elem_2]))

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.requests.Response.json')
    def test_fetch_for_language_one_return_value(self, mock_json, mock_app_id):

        mock_json.return_value = {'results': [response_elem_1]}

        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertCountEqual([response_elem_1], res)

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.requests.Response.json')
    def test_fetch_for_lang_multi_return_values(self, mock_json, mock_app_id):

        mock_json.return_value = {
            'results': [response_elem_1, response_elem_2]
        }

        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertCountEqual([response_elem_1, response_elem_2], res)

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.requests.Response.json')
    def test_fetch_for_lang_no_reviews_returned(self, mock_json, mock_app_id):

        mock_json.return_value = {'results': []}

        res = FetchGplayReviews().fetch_for_language('xyz')

        self.assertCountEqual([], res)

    @patch('gplay_reviews.FetchGplayReviews.get_app_id',
           return_value='com.barberini.museum.barberinidigital')
    @patch('gplay_reviews.requests.get')
    def test_fetch_for_language_request_failed(self, mock_get, mock_app_id):

        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 400

        self.assertRaises(
            requests.exceptions.HTTPError,
            FetchGplayReviews().fetch_for_language, 'xyz'
        )

    def test_get_language_codes(self):

        language_codes = FetchGplayReviews().get_language_codes()

        self.assertTrue(all(
            isinstance(code, str) and len(code) <= 5 
            for code in language_codes)
        )
        self.assertGreater(len(language_codes), 50)

    @patch.object(FetchGplayReviews, 'input')
    def test_get_app_id(self, input_mock):

        input_target = MockTarget('museum_facts', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as fp:
            json.dump({'ids': {'gplay': {'appId': 'some ID'}}}, fp)

        app_id = FetchGplayReviews().get_app_id()

        self.assertEqual(app_id, 'some ID')

    def test_convert_to_right_output_format(self):

        reviews = pd.DataFrame([response_elem_1])

        actual = FetchGplayReviews().convert_to_right_output_format(reviews)

        expected = pd.DataFrame([response_elem_1_renamed_cols])
        pd.testing.assert_frame_equal(expected, actual)

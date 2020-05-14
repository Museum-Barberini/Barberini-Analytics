import json
import pandas as pd
import requests

from luigi.format import UTF8
from luigi.mock import MockTarget
from unittest.mock import patch

from db_test import DatabaseTestCase
from gplay.gplay_reviews import FetchGplayReviews


RESPONSE_ELEM_1 = {
    'id': '123a',
    'date': '2020-01-04T17:09:33.789Z',
    'score': 4,
    'text': 'elem 1 text üÄö',
    'title': 'elem 2 title',
    'thumbsUp': 0,
    'version': '2.10.7',
    'app_id': 'com.barberini.museum.barberinidigital'
}
RESPONSE_ELEM_1_RENAMED_COLS = {
    'playstore_review_id': '123a',
    'text': 'elem 1 text üÄö',
    'rating': 4,
    'app_version': '2.10.7',
    'likes': 0,
    'title': 'elem 2 title',
    'date': '2020-01-04T17:09:33.789Z',
    'app_id': 'com.barberini.museum.barberinidigital'
}
RESPONSE_ELEM_2 = {
    'id': "abc",
    'date': '2019-02-24T09:20:00.123Z',
    'score': 0,
    'text': 'elem 2 text',
    'title': 'elem 2 title',
    'thumbsUp': 9,
    'version': '1.1.2',
    'app_id': 'com.barberini.museum.barberinidigital'
}


class TestFetchGplayReviews(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.task = FetchGplayReviews()
        self.task._app_id = 'com.barberini.museum.barberinidigital'

    @patch('gplay.gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de'])
    @patch('gplay.gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[[RESPONSE_ELEM_1], [RESPONSE_ELEM_1], []])
    @patch.object(FetchGplayReviews, 'output')
    @patch.object(FetchGplayReviews, 'input')
    def test_run(self, input_mock, output_mock, mock_fetch, mock_lang):

        input_target = MockTarget('museum_facts', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as fp:
            json.dump(
                {'ids': {'gplay': {
                    'appId': 'com.barberini.museum.barberinidigital'
                }}},
                fp
            )
        output_target = MockTarget('gplay.gplay_reviews', format=UTF8)
        output_mock.return_value = output_target

        FetchGplayReviews().run()

        expected = pd.DataFrame([RESPONSE_ELEM_1_RENAMED_COLS])
        with output_target.open('r') as output_file:
            actual = pd.read_csv(output_file)

        pd.testing.assert_frame_equal(expected, actual)

    def test_fetch_language_actual_data(self):

        reviews_en = self.task.fetch_for_language('en')

        self.assertTrue(reviews_en)
        keys = [
            'app_id', 'id', 'date', 'score',
            'text', 'title', 'thumbsUp', 'version'
        ]
        for review in reviews_en:
            self.assertCountEqual(keys, review.keys())

    def test_fetch_language_actual_data_wrong_country_code(self):

        reviews = self.task.fetch_for_language('nonexisting lang')
        reviews_en = self.task.fetch_for_language('en')

        # If the supplied language code does not exist, english reviews
        # are returned.This test ensures that we notice if the
        # behaviour changes.
        self.assertEqual(reviews, reviews_en)

    @patch('gplay.gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[[RESPONSE_ELEM_1], [RESPONSE_ELEM_2], []])
    @patch('gplay.gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de', 'fr'])
    def test_fetch_all_multiple_return_values(
            self, mock_lang, m_app_idtch):

        result = self.task.fetch_all()

        self.assertIsInstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(
            result, pd.DataFrame([RESPONSE_ELEM_1, RESPONSE_ELEM_2]))

    @patch('gplay.gplay_reviews.FetchGplayReviews.fetch_for_language',
           return_value=[])
    def test_fetch_lang_all_results_are_empty(self, mock_fetch):

        result = self.task.fetch_all()

        pd.testing.assert_frame_equal(
            result,
            pd.DataFrame(columns=[
                'id', 'date', 'score', 'text', 'title',
                'thumbsUp', 'version', 'app_id'
            ])
        )

    @patch('gplay.gplay_reviews.FetchGplayReviews.fetch_for_language',
           side_effect=[
               [RESPONSE_ELEM_1, RESPONSE_ELEM_2, RESPONSE_ELEM_2],
               [RESPONSE_ELEM_1]
           ])
    @patch('gplay.gplay_reviews.FetchGplayReviews.get_language_codes',
           return_value=['en', 'de'])
    def test_fetch_all_no_duplicates(self, mock_lang, mock_fetch):

        result = FetchGplayReviews().fetch_all()

        pd.testing.assert_frame_equal(
            result, pd.DataFrame([RESPONSE_ELEM_1, RESPONSE_ELEM_2]))

    @patch('gplay.gplay_reviews.requests.Response.json')
    def test_fetch_for_language_one_return_value(self, mock_json):

        mock_json.return_value = {'results': [RESPONSE_ELEM_1]}

        self.task = FetchGplayReviews()
        result = FetchGplayReviews().fetch_for_language('xyz')

        self.assertCountEqual([RESPONSE_ELEM_1], result)

    @patch('gplay.gplay_reviews.requests.Response.json')
    def test_fetch_for_lang_multi_return_values(self, mock_json):

        mock_json.return_value = {
            'results': [RESPONSE_ELEM_1, RESPONSE_ELEM_2]
        }

        result = self.task.fetch_for_language('xyz')

        self.assertCountEqual([RESPONSE_ELEM_1, RESPONSE_ELEM_2], result)

    @patch('gplay.gplay_reviews.requests.Response.json')
    def test_fetch_for_lang_no_reviews_returned(self, mock_json):

        mock_json.return_value = {'results': []}

        result = self.task.fetch_for_language('xyz')

        self.assertCountEqual([], result)

    @patch('gplay.gplay_reviews.requests.get')
    def test_fetch_for_language_request_failed(self, mock_get):

        mock_get.return_value = requests.Response()
        mock_get.return_value.status_code = 400

        with self.assertRaises(requests.exceptions.HTTPError):
            self.task.fetch_for_language('xyz')

    def test_get_language_codes(self):

        language_codes = FetchGplayReviews().get_language_codes()

        self.assertTrue(all(
            isinstance(code, str) and len(code) <= 5
            for code in language_codes)
        )
        self.assertGreater(len(language_codes), 50)

    @patch.object(FetchGplayReviews, 'input')
    def test_app_id(self, input_mock):

        input_target = MockTarget('museum_facts', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as fp:
            json.dump({'ids': {'gplay': {'appId': 'some ID'}}}, fp)
        self.task._app_id = None

        app_id = FetchGplayReviews().app_id

        self.assertEqual(app_id, 'some ID')

    def test_convert_to_right_output_format(self):

        reviews = pd.DataFrame([RESPONSE_ELEM_1])

        actual = FetchGplayReviews().convert_to_right_output_format(reviews)

        expected = pd.DataFrame([RESPONSE_ELEM_1_RENAMED_COLS])
        pd.testing.assert_frame_equal(expected, actual)

import warnings
from unittest.mock import MagicMock
import json

import googleapiclient.discovery
import pandas as pd

from db_test import DatabaseTestCase
from google_maps import FetchGoogleMapsReviews


class TestFetchGoogleMapsReviews(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.task = FetchGoogleMapsReviews()

    def test_load_credentials(self):
        credentials = self.task.load_credentials()
        self.assertIsNotNone(credentials)
        self.assertTrue(
            credentials.has_scopes(
                ['https://www.googleapis.com/auth/business.manage']))

    def test_load_credentials_missing(self):
        self.task = FetchGoogleMapsReviews(
            token_cache=self.task.token_cache
            + '.iamafilepaththathopefullydoesnotexist',
            is_interactive=False)
        with self.assertRaises(Exception) as context:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')
                self.task.load_credentials()
        self.assertTrue('credentials' in str(context.exception))

    # KNOWN API ISSUE: ResourceWarning: unclosed <ssl.SSLSocket ...>
    # https://github.com/googleapis/google-api-python-client/issues/618
    # This is an error of the API, not our fault. Don't fix it. Just for you
    # to know.
    def test_load_service(self):
        credentials = self.task.load_credentials()
        service = self.task.load_service(credentials)
        self.assertIsNotNone(service)
        self.assertIsInstance(service, googleapiclient.discovery.Resource)

    def test_fetch_raw_reviews(self):
        # ----- Set up test parameters -----
        account_name = 'myaccount'
        location_name = 'mylocation'
        place_id = 'abc456'
        all_reviews = [
            {'text': text, 'placeId': place_id}
            for text in [
                "Wow!",
                "The paintings are not animated",
                "Van Gogh is dead"
            ]
        ]
        page_size = 2
        page_token = None
        latest_page_token = None
        counter = 0

        service = MagicMock()
        accounts = MagicMock()
        service.accounts.return_value = accounts
        accounts.list.return_value = MagicMock()
        accounts.list().execute.return_value = {
            'accounts': [{'name': account_name}]
        }

        locations = MagicMock()
        accounts.locations.return_value = locations
        locations_list_mock = MagicMock()
        locations.list.return_value = locations_list_mock
        locations_list_mock.execute.return_value = {
            'locations': [{
                'name': location_name,
                'locationKey': {
                    'placeId': place_id
                }
            }]
        }

        reviews = MagicMock()
        locations.reviews.return_value = reviews
        reviews_list_mock = MagicMock()
        reviews.list.return_value = reviews_list_mock

        def reviews_list_execute():
            nonlocal page_token, counter, latest_page_token
            latest_page_token = page_token
            page_token = counter
            next_counter = counter + 2
            result = {
                'reviews': all_reviews[counter:next_counter],
                'totalReviewCount': len(all_reviews)
            }
            if counter < len(all_reviews):
                result['nextPageToken'] = page_token
            counter = next_counter
            return result
        reviews_list_mock.execute.side_effect = reviews_list_execute

        # ----- Execute code under test ----
        result_reviews = list(self.task.fetch_raw_reviews(service, page_size))

        # ----- Inspect result ------
        self.assertSequenceEqual(all_reviews, result_reviews)
        locations.list.assert_called_once_with(parent=account_name)
        reviews.list.assert_called_with(
            parent=location_name,
            pageSize=page_size,
            pageToken=latest_page_token)  # refers to last call

    def test_extract_reviews(self):
        with open(
            'tests/test_data/google_maps/raw_reviews.json',
            'r',
            encoding='utf-8'
                ) as raw_reviews_file:
            raw_reviews = json.load(raw_reviews_file)
        expected_extracted_reviews = pd.read_csv(
            'tests/test_data/google_maps/expected_extracted_reviews.csv')

        # ----- Execute code under test ----
        actual_extracted_reviews = self.task.extract_reviews(raw_reviews)

        # ----- Inspect result ------
        pd.testing.assert_frame_equal(
            expected_extracted_reviews,
            actual_extracted_reviews)

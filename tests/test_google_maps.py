import warnings
from unittest.mock import MagicMock
import json

import googleapiclient.discovery
import pandas as pd

from db_test import DatabaseTestCase
from google_maps import FetchGoogleMapsReviews


class TestFetchGoogleMapsReviews(DatabaseTestCase):
    """Tests the FetchGoogleMapsReviews task."""

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
    def test_load_services(self):
        credentials = self.task.load_credentials()
        services = self.task.load_services(credentials)
        self.assertGreater(len(services), 0)
        for service in services.values():
            self.assertIsNotNone(service)
            self.assertIsInstance(service, googleapiclient.discovery.Resource)

    def test_fetch_raw_reviews(self):
        # ----- Set up test parameters -----
        account_name = 'myaccount'
        location_name = 'mylocation'
        place_id = 'abc456'
        place_uri = 'https://example.com/place'
        all_reviews = [
            {'text': text, 'placeId': place_id, 'uri': place_uri}
            for text in [
                "Wow!",
                "⭐⭐⭐⭐⭐",
                "The paintings are not animated",
                "Van Gogh is dead"
            ]
        ]
        page_size = 2
        page_token = None
        latest_page_token = None
        counter = 0

        accounts_service = MagicMock()
        accounts = MagicMock()
        accounts_service.accounts.return_value = accounts
        accounts.list.return_value = MagicMock()
        accounts.list().execute.return_value = {
            'accounts': [{'name': account_name}]
        }

        businesses_service = MagicMock()
        businesses_accounts = MagicMock()
        businesses_service.accounts.return_value = businesses_accounts
        locations = MagicMock()
        businesses_accounts.locations.return_value = locations
        locations_list_mock = MagicMock()
        locations.list.return_value = locations_list_mock
        locations_list_mock.execute.return_value = {
            'locations': [{
                'name': location_name,
                'metadata': {
                    'placeId': place_id,
                    'uri': place_uri
                }
            }]
        }

        my_business_service = MagicMock()
        my_business_accounts = MagicMock()
        my_business_service.accounts.return_value = my_business_accounts
        my_business_locations = MagicMock()
        my_business_accounts.locations.return_value = my_business_locations
        reviews = MagicMock()
        my_business_locations.reviews.return_value = reviews
        reviews_list_mock = MagicMock()
        reviews.list.return_value = reviews_list_mock

        def reviews_list_execute():
            nonlocal page_token, counter, latest_page_token
            latest_page_token = page_token
            page_token = counter
            next_counter = counter + 2
            next_reviews = all_reviews[counter:next_counter]
            result = {
                'totalReviewCount': len(all_reviews)
            }
            if next_reviews:
                result['reviews'] = next_reviews
            if counter < len(all_reviews):
                result['nextPageToken'] = page_token
            counter = next_counter
            return result
        reviews_list_mock.execute.side_effect = reviews_list_execute

        services = {
            'accounts': accounts_service,
            'businesses': businesses_service,
            'my_business': my_business_service
        }

        # ----- Execute code under test ----
        result_reviews = list(self.task.fetch_raw_reviews(
            services, page_size))

        # ----- Inspect result ------
        self.assertSequenceEqual(all_reviews, result_reviews)
        locations.list.assert_called_once()
        self.assertEqual(locations.list.call_args[1]['parent'], account_name)
        reviews.list.assert_called_with(
            parent=f'{account_name}/{location_name}',
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

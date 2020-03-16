import json
import unittest
from unittest.mock import MagicMock, patch

from luigi.format import UTF8
from luigi.mock import MockTarget
from requests.exceptions import HTTPError

import facebook


class TestFacebookPost(unittest.TestCase):

    @patch('facebook.requests.get')
    @patch.object(facebook.FetchFbPosts, 'output')
    def test_post_transformation(self, output_mock, requests_get_mock):
        output_target = MockTarget('post_out', format=UTF8)
        output_mock.return_value = output_target

        with open('tests/test_data/facebook/post_actual.json', 'r') as data_in:
            actual_data = data_in.read()

        with open('tests/test_data/facebook/post_expected.csv',
                  'r') as data_out:
            expected_data = data_out.read()

        # Overwrite requests 'get' return value to provide our test data
        def mock_json():
            return json.loads(actual_data)

        mock_response = MagicMock(ok=True, json=mock_json)
        requests_get_mock.return_value = mock_response

        facebook.FetchFbPosts().run()

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)

    @patch('facebook.requests.get')
    def test_invalid_response_raises_error(self, requests_get_mock):
        error_mock = MagicMock(status_code=404)

        def error_raiser():
            return facebook.requests.Response.raise_for_status(error_mock)

        mock_response = MagicMock(
            raise_for_status=error_raiser)

        requests_get_mock.return_value = mock_response
        with self.assertRaises(HTTPError):
            facebook.FetchFbPosts().run()


class TestFacebookPostPerformance(unittest.TestCase):

    def test_post_performance_transformation(self):
        pass
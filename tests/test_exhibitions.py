import json
import unittest
from unittest.mock import MagicMock, patch

from luigi.format import UTF8
from luigi.mock import MockTarget

import exhibitions


class TestExhibitions(unittest.TestCase):

    @patch('exhibitions.requests.get')
    @patch.object(exhibitions.FetchExhibitions, 'output')
    def test_exhibition_transformation(self, output_mock, requests_get_mock):
        output_target = MockTarget('exhibition_out', format=UTF8)
        output_mock.return_value = output_target

        with open('tests/test_data/exhibitions/exhibitions_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open('tests/test_data/exhibitions/exhibitions_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        # Overwrite requests 'get' return value to provide our test data
        def mock_json():
            return json.loads(input_data)

        mock_response = MagicMock(ok=True, json=mock_json)
        requests_get_mock.return_value = mock_response

        exhibitions.FetchExhibitions().run()

        with output_target.open('r') as output_data:
            self.assertEqual(output_data.read(), expected_data)

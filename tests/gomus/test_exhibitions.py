import json
from unittest.mock import MagicMock, patch

from luigi.format import UTF8
from luigi.mock import MockTarget

from db_test import DatabaseTestCase
from gomus.exhibitions import ExhibitionTimesToDb
from gomus.exhibitions import FetchExhibitions, FetchExhibitionTimes


class TestExhibitions(DatabaseTestCase):
    """Tests the gomus exhibition tasks."""

    @patch('gomus.exhibitions.requests.get')
    @patch.object(FetchExhibitions, 'output')
    def test_exhibitions(self, output_mock, requests_get_mock):

        output_target = MockTarget('exhibition_out', format=UTF8)
        output_mock.return_value = output_target

        with open('tests/test_data/gomus/exhibitions/exhibitions_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open('tests/test_data/gomus/exhibitions/'
                  'exhibitions_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        # Overwrite requests 'get' return value to provide our test data
        def mock_json():
            return json.loads(input_data)

        mock_response = MagicMock(ok=True, json=mock_json)
        requests_get_mock.return_value = mock_response

        FetchExhibitions().run()

        with output_target.open('r') as output_data:
            self.assertEqual(expected_data, output_data.read())

    @patch('gomus.exhibitions.requests.get')
    @patch.object(FetchExhibitionTimes, 'output')
    def test_exhibition_times(self, output_mock, requests_get_mock):

        output_target = MockTarget('exhibition_out', format=UTF8)
        output_mock.return_value = output_target

        with open('tests/test_data/gomus/exhibitions/exhibitions_actual.json',
                  'r',
                  encoding='utf-8') as data_in:
            input_data = data_in.read()

        with open('tests/test_data/gomus/exhibitions/'
                  'exhibition_times_expected.csv',
                  'r',
                  encoding='utf-8') as data_out:
            expected_data = data_out.read()

        # Overwrite requests 'get' return value to provide our test data
        def mock_json():
            return json.loads(input_data)

        mock_response = MagicMock(ok=True, json=mock_json)
        requests_get_mock.return_value = mock_response

        FetchExhibitionTimes().run()

        with output_target.open('r') as output_data:
            self.assertEqual(expected_data, output_data.read())

    def test_short_title(self):

        self.task = ExhibitionTimesToDb()
        self.run_task(self.task)

        ambiguous_titles = self.db_connector.query('''
            SELECT short_title
            FROM exhibition
            GROUP BY short_title
            HAVING COUNT(title) <> 1
        ''')

        self.assertFalse(
            ambiguous_titles,
            msg=f"These exhibition titles are ambiguous: {ambiguous_titles}"
        )

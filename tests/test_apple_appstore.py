from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from apple_appstore import FetchAppstoreReviews, AppstoreReviewsToDb
from db_test import DatabaseTestCase
from _utils import MuseumFacts


FAKE_COUNTRY_CODES = ['DE', 'US', 'PL', 'BB']
XML_FRAME = '''<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns:im="http://itunes.apple.com/rss"
        xmlns="http://www.w3.org/2005/Atom" xml:lang="de">
    <link rel="not-next" href="this is some other link in the response;
        we don't ever want to access this"/>
    <link rel="next" href="https://itunes.apple.com/de/rss/customerreviews/\
        page=2/id=288286261/sortby=mostrecent/xml?urlDesc=/customerreviews/\
            id=288286261/mostrecent/xml"/>
    %s
    </feed>'''
XML_EMPTY_FRAME = XML_FRAME % ''


class TestFetchAppleReviews(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.task = FetchAppstoreReviews()
        self.run_task(MuseumFacts())
        self.task.get_country_codes = lambda: FAKE_COUNTRY_CODES

    def test_germany_basic(self):
        self.task.requires().run()  # workaround
        result = self.task.fetch_for_country('DE')
        self.assertIsInstance(result, pd.DataFrame)

    @patch('apple_appstore.requests.get')
    def test_get_request_returns_bad_status_code(self, mock):
        def raise_for_all_cases():
            raise requests.HTTPError
        mock.return_value = MagicMock(raise_for_status=raise_for_all_cases)
        self.assertRaises(
            requests.HTTPError,
            self.task.fetch_for_country,
            'de')

    @patch('apple_appstore.requests.get')
    def test_only_one_review_fetched(self, mock):

        return_values = [
            XML_FRAME % '''<entry>
                <updated>2012-11-10T09:08:07-07:00</updated>
                <id>5483431986</id>
                <title>I'm a fish</title>
                <content type="text">
                The fish life is thug af #okboomer
                </content>
                <im:voteSum>9</im:voteSum>
                <im:voteCount>42</im:voteCount>
                <im:rating>5</im:rating>
                <im:version>2.10.7</im:version>
                <author><name>Blubb</name></author>
                <content type="html">
                <somehtml> note=&quot;We don't want to parse\
                this&quot;&gt;&lt;anIrrelevantTag /&gt;</somehtml>
                </content>
            </entry>''',
            XML_EMPTY_FRAME
        ]

        mock.side_effect = [
            MagicMock(ok=True, text=return_value)
            for return_value in return_values
        ]

        result = self.task.fetch_for_country('made_up_country')

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertListEqual([
            'app_id',
            'appstore_review_id',
            'text',
            'rating',
            'app_version',
            'vote_count',
            'vote_sum',
            'title',
            'date',
            'country_code'],
            list(result.columns))

    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_all_countries(self, mock):

        def mock_return():
            for i in range(5000):
                yield pd.DataFrame({
                    'app_id': '123456',
                    'country_code': [f'{i}'],
                    'appstore_review_id': [i]
                })
        mock.side_effect = mock_return()

        result = self.task.fetch_all()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(FAKE_COUNTRY_CODES), len(result))

        # get a list with all args passed to mock (hopefully all country ids)
        args = [args[0] for (args, _) in mock.call_args_list]
        for arg in args:
            self.assertRegex(arg, r'^\w{2}$')

    @patch.object(FetchAppstoreReviews, 'fetch_page')
    def test_http_error(self, fetch_page_mock):
        mock_res = MagicMock(status_code=503)
        fetch_page_mock.side_effect = \
            requests.exceptions.HTTPError(response=mock_res)
        try:
            self.task.fetch_for_country('made_up_country')
        except requests.exceptions.HTTPError:
            self.fail("503 HTTP Error should be caught")

        mock_res = MagicMock(status_code=403)
        fetch_page_mock.side_effect = \
            requests.exceptions.HTTPError(response=mock_res)
        try:
            self.task.fetch_for_country('not_DE_not_GB_not_US')
        except requests.exceptions.HTTPError:
            self.fail("403 HTTP Error should not be caught for DE, GB and US")

        with self.assertRaises(requests.exceptions.HTTPError):
            self.task.fetch_for_country('GB')

    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_all_countries_some_countries_dont_have_data(self, mock):

        def mock_return(country_code):
            if country_code == 'BB':
                return pd.DataFrame([])
            return pd.DataFrame({
                'app_id': '123456',
                'country_code': [country_code],
                'appstore_review_id': [country_code]
            })
        mock.side_effect = mock_return

        result = self.task.fetch_all()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(FAKE_COUNTRY_CODES) - 1, len(result))

    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_drop_duplicate_reviews(self, mock):

        def mock_return(country_code):
            if country_code == 'BB':  # simulate no available data
                return pd.DataFrame([])
            return pd.DataFrame({
                'app_id': '123456',
                'appstore_review_id': ['xyz'],
                'country_code': [country_code]
            })
        mock.side_effect = mock_return

        result = self.task.fetch_all()

        self.assertEqual(1, len(result))

    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_same_review_for_multiple_country_codes(
            self, mock_fetch_for_country):

        mock_fetch_for_country_return = [
            pd.DataFrame({
                'app_id': '123456',
                'appstore_review_id': ['1', '2'],
                'text': ['C_1', 'C_2'],
                'rating': ['R_1', 'R_2'],
                'app_version': ['AV_1', 'AV_2'],
                'vote_count': [1, 2],
                'vote_sum': [1, 2],
                'title': ['T_2', 'T_2'],
                'country_code': ['AB', 'AB']
            }),
            pd.DataFrame({
                'app_id': '123456',
                'appstore_review_id': ['1'],
                'text': ['C_1'],
                'rating': ['R_1'],
                'app_version': ['AV_1'],
                'vote_count': [1],
                'vote_sum': [1],
                'title': ['T_2'],
                'country_code': ['CD']
            })
        ]
        mock_fetch_for_country.side_effect = mock_fetch_for_country_return
        self.task.get_country_codes = lambda: ['AB', 'CD']

        result = self.task.fetch_all()
        self.assertEqual(len(result), 2)
        pd.testing.assert_frame_equal(
            pd.DataFrame({
                'app_id': '123456',
                'appstore_review_id': ['1', '2'],
                'text': ['C_1', 'C_2'],
                'rating': ['R_1', 'R_2'],
                'app_version': ['AV_1', 'AV_2'],
                'vote_count': [1, 2],
                'vote_sum': [1, 2],
                'title': ['T_2', 'T_2']
            }),
            result.drop(columns=['country_code'])
        )


class TestAppstoreReviewsToDb(DatabaseTestCase):

    @patch.object(FetchAppstoreReviews, 'get_country_codes')
    @patch('apple_appstore.requests.get')
    def test_umlauts(self, requests_mock, country_codes_mock):

        umlaut_title = "Än ümlaut cömment"
        umlaut_text = (
            "Süßölgefäß "
            "Großfräsmaschinenöffnungstür "
            "Grießklößchensüppchenschälchen")
        umlaut_author = "Ölrückstoßabdämpfung"
        return_values = [
            XML_FRAME % f'''<entry>
                <updated>2012-11-10T09:08:07-07:00</updated>
                <id>5483431986</id>
                <title>{umlaut_title}</title>
                <content type="text">{umlaut_text}</content>
                <content type="html">
                    &lt;somethtml note=&quot;We don't want to parse\
                    this&quot;&gt;&lt;anIrrelevantTag /&gt;&lt;/somehtml&gt;
                </content>
                <im:voteSum>-42</im:voteSum>
                <im:voteCount>42</im:voteCount>
                <im:rating>1</im:rating>
                <im:version>1.2.3</im:version>
                <author><name>{umlaut_author}</name></author>
            </entry>''',
            XML_EMPTY_FRAME
        ]

        class MockResponse(requests.Response):

            def __init__(self, mock_content):
                super().__init__()
                self.mock_content = mock_content
                self.status_code = 200  # OK

            mock_content = None

            @property
            def content(self):
                return bytearray(self.mock_content, encoding='utf-8')

            @property
            def apparent_encoding(self):
                """
                By overriding this property, we simulate the behavior of
                Response/chardet to detect a wrong encoding. This happened a
                few times in production.
                ptcp154 encoding (Kazakh) is definitely the wrong encoding.
                """
                return 'ptcp154'

        requests_mock.side_effect = [
            MockResponse(return_value)
            for return_value in return_values]
        country_codes_mock.return_value = ['DE']

        self.task = AppstoreReviewsToDb()
        self.run_task(self.task)

        result = self.db_connector.query(
            'SELECT title, text FROM appstore_review')
        self.assertListEqual(
            [(umlaut_title, umlaut_text)],
            result,
            msg="Encodings differ")

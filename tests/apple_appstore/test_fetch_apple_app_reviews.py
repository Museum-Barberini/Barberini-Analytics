import pandas as pd
import requests
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from src.fetch_apple_app_reviews import *


FAKE_COUNTRY_CODES = ['DE', 'US', 'PL', 'BB']


class TestFetchAppleReviews(unittest.TestCase):
    
    def setUp(self):
        super().setUp()
        self.task = FetchAppstoreReviews()
        self.task.get_country_codes = lambda: FAKE_COUNTRY_CODES

    def test_germany_basic(self):
        result = self.task.fetch_for_country('DE')
        self.assertIsInstance(result, pd.DataFrame)
    
    @patch('src.apple_appstore.fetch_apple_app_reviews.requests.get')
    def test_get_request_returns_bad_status_code(self, mock):
        def raise_for_all_cases():
            raise requests.HTTPError
        mock.return_value = MagicMock(raise_for_status=raise_for_all_cases)
        self.assertRaises(requests.HTTPError, self.task.fetch_for_country, 'de')
    
    @patch('src.apple_appstore.fetch_apple_app_reviews.requests.get')
    def test_only_one_review_fetched(self, mock):
        
        first_return = '''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns:im="http://itunes.apple.com/rss" xmlns="http://www.w3.org/2005/Atom" xml:lang="de">
	<id>https://itunes.apple.com/de/rss/customerreviews/id=1150432552/mostrecent/xml</id>
    <title>iTunes Store: Rezensionen</title>
    <updated>2020-02-04T02:59:47-07:00</updated>
    <link rel="not-next" href="this is some other link in the response; we don't ever want to access this"/>
    <link rel="next" href="https://itunes.apple.com/de/rss/customerreviews/page=2/id=1150432552/sortby=mostrecent/xml?urlDesc=/customerreviews/id=1150432552/mostrecent/xml"/>
    
    <entry>
        <updated>2012-11-10T09:08:07-07:00</updated>
        <id>5483431986</id>
        <title>I'm a fish</title>
        <content type="text">The fish life is thug af #okboomer</content>
        <im:voteSum>9</im:voteSum>
        <im:voteCount>42</im:voteCount>
        <im:rating>5</im:rating>
        <im:version>2.10.7</im:version>
        <author><name>Blubb</name></author>
        <content type="html">&lt;somethtml note=&quot;We don't want to parse this&quot;&gt;&lt;anIrrelevantTag /&gt;&lt;/somehtml&gt;</content>
    </entry>

</feed>'''
        
        second_return = '''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns:im="http://itunes.apple.com/rss" xmlns="http://www.w3.org/2005/Atom" xml:lang="de">
	<id>https://itunes.apple.com/de/rss/customerreviews/id=1150432552/mostrecent/xml</id>
    <title>iTunes Store: Rezensionen</title>
    <updated>2020-02-04T02:59:47-07:00</updated>
    <link rel="not-next" href="this is some other link in the response; we don't ever want to access this"/>
    <link rel="next" href="https://itunes.apple.com/de/rss/customerreviews/page=2/id=1150432552/sortby=mostrecent/xml?urlDesc=/customerreviews/id=1150432552/mostrecent/xml"/>
    </feed>'''
        
        mock.side_effect = [
            MagicMock(ok = True, text = first_return),
            MagicMock(ok = True, text = second_return)
        ]
        
        result = self.task.fetch_for_country('made_up_country')
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertListEqual(
		['appstore_review_id','text','rating',
		 'app_version','vote_count','vote_sum','title', 'date', 'country_code'],
		list(result.columns)
	)
    
    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_all_countries(self, mock):
        
        def mock_return():
            for i in range(5000):
                yield pd.DataFrame({'country_code': [f'{i}'], 'appstore_review_id': [i]})
        mock.side_effect = mock_return()
        
        result = self.task.fetch_all()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), len(FAKE_COUNTRY_CODES))
        
        # get a list with all args passed to the mock (hopefully all country ids)
        args = [args[0] for (args, _) in mock.call_args_list]
        for arg in args:
            self.assertRegex(arg, r'^\w{2}$')
    
    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_all_countries_some_countries_dont_have_data(self, mock):
        
        def mock_return(country_code):
            if country_code == 'BB':
                raise ValueError()
            return pd.DataFrame({'country_code': [country_code], 'appstore_review_id': [country_code]})
        mock.side_effect = mock_return
        
        result = self.task.fetch_all()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), len(FAKE_COUNTRY_CODES) - 1)
    
    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_drop_duplicate_reviews(self, mock):
        
        def mock_return(country_code):
            if country_code == 'BB': # simulate no available data
                raise ValueError()
            return pd.DataFrame({'appstore_review_id': ['xyz'], 'country_code': [country_code]})
        mock.side_effect = mock_return
        
        result = self.task.fetch_all()
        
        self.assertEqual(len(result), 1)
    
    @patch.object(FetchAppstoreReviews, 'fetch_for_country')
    def test_same_review_for_multiple_country_codes(self, mock_fetch_for_country):
        
        mock_fetch_for_country_return = [
            pd.DataFrame({
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
                'appstore_review_id': ['1'],
                'text': ['C_1'],
                'rating': ['R_1'],
                'app_version': ['AV_1'],
                'vote_count': [1],
                'vote_sum': [1],
                'title': ['T_2' ],
                'country_code': ['CD']
            })
        ]
        mock_fetch_for_country.side_effect = mock_fetch_for_country_return
        self.task.get_country_codes = lambda: ['AB', 'CD']
        
        result = self.task.fetch_all()
        self.assertEqual(len(result), 2)
        pd.testing.assert_frame_equal(
            pd.DataFrame({
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


import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from src.apple_appstore.fetch_apple_app_reviews import *
import pandas as pd
import requests


class MockRequest:
    def __init__(self):
            self.status_code = 450


class TestFetchAppleReviews(unittest.TestCase):
    
    def test_germany_basic(self):
        result = fetch_country("DE")
        self.assertIsInstance(result, pd.DataFrame)
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.requests.get")
    def test_get_request_returns_bad_status_code(self, mock):
        mock.return_value = MagicMock(ok = False)
        self.assertRaises(requests.HTTPError, fetch_country, "de")
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.requests.get")
    def test_only_one_review_fetched(self, mock):
        
        first_return = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns:im="http://itunes.apple.com/rss" xmlns="http://www.w3.org/2005/Atom" xml:lang="de">
	<id>https://itunes.apple.com/de/rss/customerreviews/id=1150432552/mostrecent/xml</id>
    <title>iTunes Store: Rezensionen</title>
    <updated>2020-02-04T02:59:47-07:00</updated>
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

</feed>"""
        
        second_return = ""
        
        mock.side_effect = [
                MagicMock(ok = True, json = lambda: first_return),
                MagicMock(ok = True, json = lambda: second_return)
        ]
        
        result = fetch_country("made_up_country")
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertListEqual(
		["id","content","rating",
		 "app_version","vote_count","vote_sum","title", "date", "country_code"],
		list(result.columns)
	)
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
    def test_all_countries(self, mock):
        
        def mock_return():
                for i in range(5000):
                        yield pd.DataFrame({"countryId": [f"{i}"], "id": [i]})
        mock.side_effect = mock_return()
        
        result = fetch_all()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 250)
        
        # get a list with all args passed to the mock (hopefully all country ids)
        args = [args[0] for (args, _) in mock.call_args_list]
        for arg in args:
                self.assertRegex(arg, r"^\w{2}$")
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
    def test_all_countries_some_countries_dont_have_data(self, mock):
         
        def mock_return(country_code):
                if country_code == "BB":
                        raise ValueError()
                return pd.DataFrame({"countryId": [country_code], "id": [country_code]})
        mock.side_effect = mock_return
        
        result = fetch_all()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 249)
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
    def test_drop_duplicate_tweets(self, mock):
        
        def mock_return(country_code):
            if country_code == "BB":
                raise ValueError()
            return pd.DataFrame({"id": ["xyz"], "countryId": [country_code]})
        mock.side_effect = mock_return
        
        result = fetch_all()
        
        self.assertEqual(len(result), 1)
    
    @patch("src.apple_appstore.fetch_apple_app_reviews.get_country_codes")
    @patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
    def test_same_review_for_multiple_country_codes(self, mock_fetch_country, mock_get_country_codes):
        
        mock_fetch_country_return = [
            pd.DataFrame({
                "id": ["1", "2"],
                "content": ["C_1", "C_2"],
                "content_type": ["CT_1", "CT_2"],
                "rating": ["R_1", "R_2"],
                "app_version": ["AV_1", "AV_2"],
                "vote_count": [1, 2],
                "vote_sum": [1, 2],
                "title": ["T_2", "T_2"],
                "countryId": ["AB", "AB"]
            }),
            pd.DataFrame({
                "id": ["1"],
                "content": ["C_1"],
                "content_type": ["CT_1"],
                "rating": ["R_1"],
                "app_version": ["AV_1"],
                "vote_count": [1],
                "vote_sum": [1],
                "title": ["T_2" ],
                "countryId": ["CD"]
            })
        ]
        mock_fetch_country.side_effect = mock_fetch_country_return
        mock_get_country_codes.return_value = ["AB", "CD"]
        
        result = fetch_all()
        
        self.assertEqual(len(result), 2)
        pd.util.testing.assert_frame_equal(
            pd.DataFrame({
                "id": ["1", "2"],
                "content": ["C_1", "C_2"],
                "content_type": ["CT_1", "CT_2"],
                "rating": ["R_1", "R_2"],
                "app_version": ["AV_1", "AV_2"],
                "vote_count": [1, 2],
                "vote_sum": [1, 2],
                "title": ["T_2", "T_2"]
            }),
            result
        )


if __name__ == '__main__':
    unittest.main()

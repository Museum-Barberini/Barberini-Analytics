import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from src.fetch_apple_app_reviews import *
import pandas as pd
import requests


class MockRequest:
    def __init__(self):
            self.status_code = 450


class TestFetchAppleReviews(unittest.TestCase):
    
    def test_germany_basic(self):
        result = fetch_country("DE")
        self.assertIsInstance(result, pd.DataFrame)
    
    @patch("src.fetch_apple_app_reviews.requests.get")
    def test_get_request_returns_bad_status_code(self, mock):
        mock.return_value = MagicMock(ok = False)
        self.assertRaises(requests.HTTPError, fetch_country, "de")
    
    @patch("src.fetch_apple_app_reviews.requests.get")
    def test_only_one_review_fetched(self, mock):
        
        first_return = {
                "feed": {
                        "entry": {
                                "author": {"name": {"label": "Blubb"}},
                                "id": {"label": "id value"},
                                "content": {
                                        "label": "The fish life is thug af #okboomer",
                                        "attributes": {"type": "content_type value"}
                                },
                                "im:rating": {"label": "1"},
                                "im:version": {"label": "-1000"},
                                "im:voteCount": {"label": "888888888888888"},
                                "im:voteSum": {"label": "-88888888888888888888888888"},
                                "title": {"label": "I'm a fish"}
                        },
                        "link": [{"attributes": {"rel": "next", "href": "some url"}}]
                }
        }
        second_return = {
                "feed": {}
        }
        
        mock.side_effect = [
                MagicMock(ok = True, json = lambda: first_return),
                MagicMock(ok = True, json = lambda: second_return)
        ]
        
        result = fetch_country("made_up_country")
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertListEqual(
        ["id","content","content_type","rating",
         "app_version","vote_count","vote_sum","title", "countryId"],
        list(result.columns)
    )
    
    @patch("src.fetch_apple_app_reviews.fetch_country")
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
    
    @patch("src.fetch_apple_app_reviews.fetch_country")
    def test_all_countries_some_countries_dont_have_data(self, mock):
         
        def mock_return(country_code):
                if country_code == "BB":
                        raise ValueError()
                return pd.DataFrame({"countryId": [country_code], "id": [country_code]})
        mock.side_effect = mock_return
        
        result = fetch_all()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 249)
    
    @patch("src.fetch_apple_app_reviews.fetch_country")
    def test_drop_duplicate_tweets(self, mock):
        
        def mock_return(country_code):
            if country_code == "BB":
                raise ValueError()
            return pd.DataFrame({"id": ["xyz"], "countryId": [country_code]})
        mock.side_effect = mock_return
        
        result = fetch_all()
        
        self.assertEqual(len(result), 1)
    
    @patch("src.fetch_apple_app_reviews.get_country_codes")
    @patch("src.fetch_apple_app_reviews.fetch_country")
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


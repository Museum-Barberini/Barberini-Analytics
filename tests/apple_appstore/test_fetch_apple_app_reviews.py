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
		self.assertIn("countryId", result.columns)
	
	@patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
	def test_all_countries(self, mock):
		def mock_return():
			for i in range(5000):
				yield pd.DataFrame({"x": [i], "id": [i]})
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
			return pd.DataFrame({"x": [country_code], "id": [country_code]})
		mock.side_effect = mock_return
		
		result = fetch_all()
		
		self.assertIsInstance(result, pd.DataFrame)
		self.assertEqual(len(result), 249)
	
	@patch("src.apple_appstore.fetch_apple_app_reviews.fetch_country")
	def test_drop_duplicate_tweets(self, mock):
		def mock_return(country_code):
			if country_code == "BB":
				raise ValueError()
			return pd.DataFrame({"id": ["xyz"], "x": [country_code]})
		mock.side_effect = mock_return
		
		result = fetch_all()
		
		self.assertEqual(len(result), 1)


if __name__ == '__main__':
	unittest.main()

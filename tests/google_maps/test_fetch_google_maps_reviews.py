import unittest
from unittest.mock import MagicMock
from fetch_google_maps_reviews import FetchGoogleMapsReviews
import googleapiclient.discovery

class TestFetchGoogleMapsReviews(unittest.TestCase):
	
	def setUp(self):
		super().setUp()
		self.task = FetchGoogleMapsReviews()
	
	def test_load_credentials(self):
		credentials = self.task.load_credentials()
		self.assertIsNotNone(credentials)
		self.assertTrue(credentials.has_scopes(['https://www.googleapis.com/auth/business.manage']))
	
	# TODO: This causes a warning: unclosed <ssl.SSLSocket ...>
	# we do not see the reason as it does not occur in the actual execution 
	def test_load_service(self):
		credentials = self.task.load_credentials()
		service = self.task.load_service(credentials)
		self.assertIsNotNone(service)
		self.assertIsInstance(service, googleapiclient.discovery.Resource)
	
	def test_fetch_raw_reviews(self):
		account_name = 'myaccount'
		location_name = 'mylocation'
		all_reviews = ["Wow!", "The paintings are not animated", "Van Gogh is dead"]
		page_size = 2
		page_token = None
		counter = 0
		
		service = MagicMock()
		service.accounts.return_value = accounts = MagicMock()
		accounts.list.return_value = MagicMock()
		accounts.list().execute.return_value = {
			'accounts': [{ 'name': account_name }]
		}
		
		accounts.locations.return_value = locations = MagicMock()
		locations_list_mock = MagicMock()
		def locations_list(parent):
			self.assertEqual(account_name, parent)
			return locations_list_mock
		locations.list.side_effect = locations_list
		locations_list_mock.execute.return_value = {
			'locations': [{ 'name': location_name }]
		}
		
		locations.reviews.return_value = reviews = MagicMock()
		reviews_list_mock = MagicMock()
		def reviews_list(parent, pageSize, pageToken=None):
			self.assertEqual(page_token, pageToken)
			self.assertEqual(location_name, parent)
			self.assertEqual(page_size, pageSize)
			return reviews_list_mock
		reviews.list.side_effect = reviews_list
		def reviews_list_execute():
			nonlocal page_token, counter
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
		
		result = self.task.fetch_raw_reviews(service, page_size)
		self.assertSequenceEqual(all_reviews, result)

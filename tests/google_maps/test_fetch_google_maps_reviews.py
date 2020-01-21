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
		
		service = MagicMock()
		service.accounts.return_value = accounts = MagicMock()
		accounts.list.return_value = MagicMock()
		accounts.list.execute.return_value = {
			'accounts': [{ 'name': account_name }]
		}
		
		# DEBUG
		account_list = service.accounts().list().execute()
		account = account_list['accounts'][0]['name']
		print('######################################' + str(account))
		# GUDEB
		
		
		
		accounts.locations.return_value = locations = MagicMock()
		locations_list_mock = MagicMock()
		def locations_list(parent):
			self.assertEquals(account_name, parent['name'])
			return locations_list_mock
		locations.list = locations_list
		locations_list_mock.execute.return_value = {
			'locations': [{ 'name': location_name }]
		}
		
		locations.reviews.return_value = reviews = MagicMock()
		reviews_list_mock = MagicMock()
		def reviews_list(parent, some_page_size, some_page_token=None):
			self.assertEquals(page_token, some_page_token)
			self.assertEquals(location_name, parent['name'])
			self.assertEquals(some_page_size, page_size)
			return reviews_list_mock
		reviews.list = reviews_list
		def reviews_list_execute():
			page_token = counter
			result = {
				'reviews': all_reviews[counter:counter + 2],
				'totalReviewCount': len(all_reviews),
				'nextPageToken': page_token
			}
			counter += 2
			return result
		
		result = self.task.fetch_raw_reviews(service, page_size)
		self.assertSequenceEqual(all_reviews, result)

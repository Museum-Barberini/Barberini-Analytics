import luigi

from csv_to_db import CsvToDb
from fetch_google_maps_reviews import FetchGoogleMapsReviews

class GoogleMapsReviewsToDB(CsvToDb):
	
	table = 'google_maps_review'
	
	columns = [
		('id', 'TEXT'),
		('date', 'DATE'),
		('rating', 'INT'),
		('content', 'TEXT'),
		('content_original', 'TEXT')
	]
	
	primary_key = 'id'
	
	def requires(self):
		return FetchGoogleMapsReviews()

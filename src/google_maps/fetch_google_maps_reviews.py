import luigi
from oauth2client.file import Storage
from oauth2client.client import OAuth2WebServerFlow
import googleapiclient.discovery
import json
import pandas as pd


class FetchGoogleMapsReviews(luigi.Task):
	
	token_cache = luigi.Parameter(default='secret_files/google_gmb_credential_cache.json')
	client_secret = luigi.Parameter(default='secret_files/google_gmb_client_secret.json')
	scopes = ['https://www.googleapis.com/auth/business.manage']
	google_gmb_discovery_url = 'https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p5.json'
	
	api_service_name = 'mybusiness'
	api_version = 'v4'
	
	stars_dict = dict({ # google returns rating as a string
		'STAR_RATING_UNSPECIFIED': None,
		'ONE': 1,
		'TWO': 2,
		'THREE': 3,
		'FOUR': 4,
		'FIVE': 5
	})
	
	def output(self):
		return luigi.LocalTarget('output/google_maps/maps_reviews.csv', format=luigi.format.UTF8)
	
	def run(self):
		print("loading credentials...")
		credentials = self.load_credentials()
		print("creating service...")
		service = self.load_service(credentials)
		print("fetching reviews...")
		raw_reviews = self.fetch_raw_reviews(service)
		print("extracting reviews...")
		reviews_df = self.extract_reviews(raw_reviews)
		print("success! writing...")
		
		with self.output().open('w') as output_file:
			reviews_df.to_csv(output_file, index=False)
	
	def load_credentials(self):
		storage = Storage(self.token_cache)
		
		credentials = storage.get()
		
		if not credentials:
			
			with open(self.client_secret) as client_secret:
				secret = json.load(client_secret)['installed']
			flow = OAuth2WebServerFlow(secret['client_id'], secret['client_secret'], self.scopes, secret['redirect_uris'][0])
			
			authorize_url = flow.step1_get_authorize_url()
			print('Go to the following link in your browser: ' + authorize_url)
			code = input('Enter verification code: ').strip()
			
			credentials = flow.step2_exchange(code)
			storage.put(credentials)
		return credentials
	
	def load_service(self, credentials):
		return googleapiclient.discovery.build(self.api_service_name, self.api_version,
			credentials=credentials, 
			discoveryServiceUrl=self.google_gmb_discovery_url)
	
	def fetch_raw_reviews(self, service, page_size=100):
		# get account identifier
		account_list = service.accounts().list().execute()
		account = account_list['accounts'][0]['name'] # in almost all cases one only has access to one account
		
		# get location identifier of the first location available to this account
		# it seems like this identifier is unique per user
		location_list = service.accounts().locations().list(parent=account).execute()
		if len(location_list['locations']) == 0:
			print("ERROR: This user seems to not have access to any google location, unable to fetch reviews")
			exit(1)
		location = location_list['locations'][0]['name']
		
		# get reviews for that location
		reviews = []
		review_list = service.accounts().locations().reviews().list(
			parent=location,
			pageSize=page_size).execute()
		reviews = reviews + review_list['reviews']
		total_reviews = review_list['totalReviewCount']
		print(f"Fetched {len(reviews)} out of {total_reviews} reviews", end='', flush=True) # creates the most beautiful loading line in the project!
		while 'nextPageToken' in review_list:
			next_page_token = review_list['nextPageToken']
			review_list = service.accounts().locations().reviews().list(
				parent=location,
				pageSize=page_size,
				pageToken=next_page_token).execute()
			reviews = reviews + review_list['reviews']
			print(f"\rFetched {len(reviews)} out of {total_reviews} reviews", end='', flush=True)
		print()
		return reviews
	
	def extract_reviews(self, raw_reviews):
		extracted_reviews = []
		for raw in raw_reviews:
			new = dict()
			new['id'] = raw['reviewId']
			new['date'] = raw['createTime']
			new['rating'] = self.stars_dict[raw['starRating']]
			new['content'] = None
			new['content_original'] = None
			
			raw_comment = raw.get('comment', None)
			if (raw_comment):
				raw_comment.replace("\n\n(Original)\n", "\n\n(Translated by Google)\n") # making google consistent with itself
				comment_pieces = raw_comment.split("\n\n(Translated by Google)\n")
				new['content'] = comment_pieces[0].strip()
				new['content_original'] = comment_pieces[-1].strip()
			extracted_reviews.append(new)
		return pd.DataFrame(extracted_reviews)

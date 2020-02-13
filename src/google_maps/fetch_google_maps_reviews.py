import googleapiclient.discovery
import json
import luigi
import oauth2client.client
import pandas as pd
import sys

from oauth2client.file import Storage

from csv_to_db import CsvToDb


class FetchGoogleMapsReviews(luigi.Task):
    
    # secret_files is a folder mounted from /etc/secrets via docker-compose
    token_cache = luigi.Parameter(default='secret_files/google_gmb_credential_cache.json')
    client_secret = luigi.Parameter(default='secret_files/google_gmb_client_secret.json')
    is_interactive = luigi.BoolParameter(default=sys.stdin.isatty())
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
    
    def run(self) -> None:
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
    
    """
    uses oauth2 to authenticate with google, also caches credentials
    requires no login action if you have a valid cache
    """
    def load_credentials(self) -> oauth2client.client.Credentials:
        storage = Storage(self.token_cache)
        credentials = storage.get()

        if credentials is None: # Access token missing or invalid, we need to require a new one
            if not self.is_interactive:
                raise Exception("ERROR: No valid credentials for google maps access and no interactive shell to perform login, aborting!")
            with open(self.client_secret) as client_secret:
                secret = json.load(client_secret)['installed']
            flow = oauth2client.client.OAuth2WebServerFlow(secret['client_id'], secret['client_secret'], self.scopes, secret['redirect_uris'][0])
            authorize_url = flow.step1_get_authorize_url()
            print("Go to the following link in your browser: " + authorize_url)
            code = input("Enter verification code: ").strip()
            credentials = flow.step2_exchange(code)
            storage.put(credentials)

        return credentials
    
    def load_service(self, credentials) -> googleapiclient.discovery.Resource:
        return googleapiclient.discovery.build(self.api_service_name, self.api_version,
            credentials=credentials, 
            discoveryServiceUrl=self.google_gmb_discovery_url)
    
    """
    the google-api is based on resources that contain other resources
    an authenticated user has account(s), an accounts contains locations and a location contains reviews (which we need to request one  by one)
    """
    def fetch_raw_reviews(self, service, page_size=100):
        # get account identifier
        account_list = service.accounts().list().execute()
        account = account_list['accounts'][0]['name'] # in almost all cases one only has access to one account
        
        # get location identifier of the first location available to this account
        # it seems like this identifier is unique per user
        location_list = service.accounts().locations().list(parent=account).execute()
        if len(location_list['locations']) == 0:
            raise Exception("ERROR: This user seems to not have access to any google location, unable to fetch reviews")
        location = location_list['locations'][0]['name']
        
        # get reviews for that location
        reviews = []
        review_list = service.accounts().locations().reviews().list(
            parent=location,
            pageSize=page_size).execute()
        reviews = reviews + review_list['reviews']
        total_reviews = review_list['totalReviewCount']
        print(f"Fetched {len(reviews)} out of {total_reviews} reviews", end='', flush=True) # creates the most beautiful loading line in the project! (so far)
        
        while 'nextPageToken' in review_list: # TODO: optimise by requesting the latest review from DB and not fetching more pages once that one is found
            next_page_token = review_list['nextPageToken']
            review_list = service.accounts().locations().reviews().list(
                parent=location,
                pageSize=page_size,
                pageToken=next_page_token).execute()
            reviews = reviews + review_list['reviews']
            print(f"\rFetched {len(reviews)} out of {total_reviews} reviews", end='', flush=True)
        print()
        return reviews
    
    def extract_reviews(self, raw_reviews) -> pd.DataFrame:
        extracted_reviews = []
        for raw in raw_reviews:
            extracted = dict()
            extracted['google_maps_review_id'] = raw['reviewId']
            extracted['date'] = raw['createTime']
            extracted['rating'] = self.stars_dict[raw['starRating']]
            extracted['text_german'] = None
            extracted['text'] = None
            
            raw_comment = raw.get('comment', None)
            if (raw_comment):
                raw_comment.replace("\n\n(Original)\n", "\n\n(Translated by Google)\n") # making google consistent with itself
                comment_pieces = raw_comment.split("\n\n(Translated by Google)\n")
                extracted['text_german'] = comment_pieces[0].strip()
                extracted['text'] = comment_pieces[-1].strip()
            extracted_reviews.append(extracted)
        return pd.DataFrame(extracted_reviews)


class GoogleMapsReviewsToDB(CsvToDb):
    
    table = 'google_maps_review'
    
    columns = [
        ('google_maps_review_id', 'TEXT'),
        ('date', 'DATE'),
        ('rating', 'INT'),
        ('text_german', 'TEXT'),
        ('text', 'TEXT')
    ]
    
    primary_key = 'google_maps_review_id'
    
    def requires(self):
        return FetchGoogleMapsReviews()

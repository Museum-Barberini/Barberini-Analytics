"""Provides tasks for downloading Google Maps reviews into the database."""

import json
import sys

import googleapiclient.discovery
import luigi
import oauth2client.client
import pandas as pd
from oauth2client.file import Storage

from _utils import CsvToDb, DataPreparationTask, logger


class GoogleMapsReviewsToDb(CsvToDb):
    """Stored fetched Google Maps reviews into the database."""

    table = 'google_maps_review'

    def requires(self):

        return FetchGoogleMapsReviews()


class FetchGoogleMapsReviews(DataPreparationTask):
    """
    Fetch reviews about the museum from Google Maps.

    Data are fetched using the Google My Business API.
    """

    # secret_files is a folder mounted from
    # /etc/barberini-analytics/secrets via docker-compose
    token_cache = luigi.Parameter(
        default='secret_files/google_gmb_credential_cache.json')
    client_secret = luigi.Parameter(
        default='secret_files/google_gmb_client_secret.json')
    is_interactive = luigi.BoolParameter(default=sys.stdin.isatty())
    scopes = ['https://www.googleapis.com/auth/business.manage']
    google_gmb_discovery_url = ('https://developers.google.com/my-business/'
                                'samples/mybusiness_google_rest_v4p5.json')

    api_service_name = 'mybusiness'
    api_version = 'v4'

    stars_dict = dict({  # Google returns rating as a string
        'STAR_RATING_UNSPECIFIED': None,
        'ONE': 1,
        'TWO': 2,
        'THREE': 3,
        'FOUR': 4,
        'FIVE': 5
    })

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/google_maps_reviews.csv',
            format=luigi.format.UTF8
        )

    def run(self) -> None:

        logger.info("loading credentials...")
        credentials = self.load_credentials()
        try:
            logger.info("creating service...")
            service = self.load_service(credentials)
            logger.info("fetching reviews...")
            raw_reviews = list(self.fetch_raw_reviews(service))
        except googleapiclient.errors.HttpError as error:
            if error.resp.status is not None:
                raise
            logger.error("Generic HTTPError raised by Google Maps. Aborting. "
                         "If you see this error message frequently, consider "
                         "to do something against it.")
            raw_reviews = []
        logger.info("extracting reviews...")
        reviews_df = self.extract_reviews(raw_reviews)
        logger.info("success! writing...")

        with self.output().open('w') as output_file:
            reviews_df.to_csv(output_file, index=False)

    def load_credentials(self) -> oauth2client.client.Credentials:
        """
        Load credentials for accessing the Google My Business API from disk.

        Uses OAuth2 to authenticate with Google, also caches credentials.
        Requires no login action when a valid cache is present.
        """
        storage = Storage(self.token_cache)
        credentials = storage.get()

        # Access token missing or invalid, we need to require a new one
        if credentials is None:
            if not self.is_interactive:
                raise Exception(
                    ("ERROR: No valid credentials for Google Maps access and "
                     "no interactive shell to perform login, aborting!"))
            with open(self.client_secret) as client_secret:
                secret = json.load(client_secret)['installed']
            flow = oauth2client.client.OAuth2WebServerFlow(
                secret['client_id'],
                secret['client_secret'],
                self.scopes,
                secret['redirect_uris'][0])
            authorize_url = flow.step1_get_authorize_url()
            logger.warning("Go to the following link in your browser: "
                           f"{authorize_url}")
            code = input("Enter verification code: ").strip()
            credentials = flow.step2_exchange(code)
            storage.put(credentials)

        return credentials

    def load_service(self, credentials) -> googleapiclient.discovery.Resource:

        return googleapiclient.discovery.build(
            self.api_service_name,
            self.api_version,
            credentials=credentials,
            discoveryServiceUrl=self.google_gmb_discovery_url)

    def fetch_raw_reviews(self, service, page_size=100):
        """
        Fetch raw reviews from the Google My Business API.

        The GMB API is based on resources that contain other resources.
        An authenticated user has account(s), an accounts contains locations,
        and a location contains reviews (which we need to request one by one).
        """
        # get account identifier
        account_list = service.accounts().list().execute()
        # in almost all cases one only has access to one account
        account = account_list['accounts'][0]['name']

        # get location identifier of the first location
        # available to this account
        # it seems like this identifier is unique per user
        location_list = service.accounts().locations()\
            .list(parent=account).execute()
        if len(location_list['locations']) == 0:
            raise Exception(
                ("ERROR: This user seems to not have access to any google "
                 "location, unable to fetch reviews"))
        location = location_list['locations'][0]['name']
        place_id = location_list['locations'][0]['locationKey']['placeId']

        # get reviews for that location
        review_list = service.accounts().locations().reviews().list(
            parent=location,
            pageSize=page_size).execute()
        yield from [
            {**review, 'placeId': place_id}
            for review
            in review_list['reviews']
        ]
        total_reviews = review_list['totalReviewCount']

        pbar_loop = iter(self.tqdm(
            range(total_reviews),
            desc="Fetching Google Maps pages"
        ))
        while 'nextPageToken' in review_list:
            next_page_token = review_list['nextPageToken']
            # TODO: optimize by requesting the latest review from DB rather
            # than fetching more pages once that one is found
            review_list = service.accounts().locations().reviews().list(
                parent=location,
                pageSize=page_size,
                pageToken=next_page_token).execute()
            for review in review_list['reviews']:
                next(pbar_loop)
            yield from [
                {**review, 'placeId': place_id}
                for review
                in review_list['reviews']
            ]

            if self.minimal_mode:
                review_list.pop('nextPageToken')

    def extract_reviews(self, raw_reviews) -> pd.DataFrame:

        return pd.DataFrame(self.extract_review(raw) for raw in raw_reviews)

    def extract_review(self, raw) -> dict:

        extracted = {
            'google_maps_review_id': raw['reviewId'],
            'post_date': raw['createTime'],
            'rating': self.stars_dict[raw['starRating']],
            'text': None,
            'text_english': None,
            'language': None,
            'place_id': raw['placeId']
        }

        raw_comment = raw.get('comment', None)
        if raw_comment:
            # This assumes possibly unintended behavior of Google's API.
            # See for details:
            # https://gitlab.hpi.de/bp-barberini/bp-barberini/issues/79
            # We want to keep both original and English translation.
            (
                extracted['text'],
                extracted['text_english'],
                extracted['language']
            ) = (
                # english reviews are as is; (Original) may be part of the text
                (raw_comment, raw_comment, "english")
                if "(Translated by Google)" not in raw_comment else (
                    # Other reviews have this format:
                    #   (Translated by Google)[english translation]\n\n
                    #   (Original)\n[original review]
                    raw_comment.split("(Original)")[1].strip(),
                    raw_comment.split("(Original)")[0].split(
                        "(Translated by Google)"
                    )[1].strip(),
                    "other"
                )
            )

        return extracted

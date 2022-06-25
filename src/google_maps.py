"""Provides tasks for downloading Google Maps reviews into the database."""

import json
import sys
from typing import Dict

import googleapiclient.discovery
import luigi
import oauth2client.client
import pandas as pd
from oauth2client.file import Storage

from _utils import CsvToDb, DataPreparationTask, logger

SERVICE_SPECS = {
    'accounts': ('mybusinessaccountmanagement', 'v1', None),
    'businesses': ('mybusinessbusinessinformation', 'v1', None),
    'my_business': (
        'mybusiness',
        'v4',
        ('https://developers.google.com/my-business/samples/'
         'mybusiness_google_rest_v4p9.json')
    )
}

STARS = dict({  # Google returns rating as a string
    'STAR_RATING_UNSPECIFIED': None,
    'ONE': 1,
    'TWO': 2,
    'THREE': 3,
    'FOUR': 4,
    'FIVE': 5
})


class GoogleMapsReviewsToDb(CsvToDb):
    """Store fetched Google Maps reviews into the database."""

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

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/google_maps_reviews.csv',
            format=luigi.format.UTF8
        )

    def run(self) -> None:

        logger.info("loading credentials...")
        credentials = self.load_credentials()
        try:
            logger.info("creating services...")
            services = self.load_services(credentials)
            logger.info("fetching reviews...")
            raw_reviews = list(self.fetch_raw_reviews(services))
        except googleapiclient.errors.HttpError as error:
            logger.warn(
                "HTTP error: %s",
                error.resp.status or "<no status code>"
            )
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
            print(
                f"Go to the following link in your browser: {authorize_url}",
                file=sys.stderr)
            code = input("Enter verification code: ").strip()
            credentials = flow.step2_exchange(code)
            storage.put(credentials)

        return credentials

    def load_services(self, credentials) -> \
            Dict[str, googleapiclient.discovery.Resource]:

        return {
            key: googleapiclient.discovery.build(
                service_name,
                version,
                credentials=credentials,
                discoveryServiceUrl=discovery_url or (
                    f'https://{service_name}.googleapis.com/'
                    f'$discovery/rest?version={version}'))
            for key, (service_name, version, discovery_url)
            in SERVICE_SPECS.items()
        }

    def fetch_raw_reviews(self, services, page_size=100):
        """
        Fetch raw reviews from the Google My Business API.

        The GMB API is based on resources that contain other resources.
        An authenticated user has account(s), an accounts contains locations,
        and a location contains reviews (which we need to request one by one).
        """
        account_list = services['accounts'].accounts().list().execute()
        assert len(account_list['accounts']) > 0

        for account in account_list['accounts']:
            account_name = account['name']

            location_list = services['businesses'].accounts().locations()\
                .list(
                    parent=account_name,
                    readMask=','.join(['name', 'metadata'])
                )\
                .execute()
            assert len(location_list['locations']) > 0

            for location in location_list['locations']:
                location_name = location['name']
                place_id = location['metadata']['placeId']
                place_uri = location['metadata']['mapsUri']
                # GMB API does not provide fine-grained permalinks. See also:
                # https://support.google.com/business/thread/11131183

                review_resource = services['my_business'].accounts()\
                    .locations().reviews()
                global_location_name = f'{account_name}/{location_name}'

                review_list = review_resource\
                    .list(parent=global_location_name, pageSize=page_size)\
                    .execute()
                yield from [
                    {**review, 'placeId': place_id, 'uri': place_uri}
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
                    # TODO: optimize by requesting the latest review from DB
                    # rather than fetching more pages once that one is found
                    review_list = review_resource\
                        .list(
                            parent=global_location_name,
                            pageSize=page_size,
                            pageToken=next_page_token)\
                        .execute()
                    try:
                        reviews = review_list['reviews']
                    except KeyError:
                        break

                    for review in reviews:
                        next(pbar_loop)
                        yield {
                            **review,
                            'placeId': place_id,
                            'uri': place_uri
                        }

                    if self.minimal_mode:
                        review_list.pop('nextPageToken')

    def extract_reviews(self, raw_reviews) -> pd.DataFrame:

        return pd.DataFrame(self.extract_review(raw) for raw in raw_reviews)

    def extract_review(self, raw) -> dict:

        extracted = {
            'google_maps_review_id': raw['reviewId'],
            'post_date': raw['createTime'],
            'rating': STARS[raw['starRating']],
            'text': None,
            'text_english': None,
            'language': None,
            'place_id': raw['placeId'],
            'uri': raw['uri']
        }

        raw_comment = raw.get('comment', None)
        if raw_comment:
            # This assumes possibly unintended behavior of Google's API.
            # See for details:
            # https://gitlab.com/Museum-Barberini/Barberini-Analytics/issues/79
            # We want to keep both original and English translation.
            # TODO: Find out whether there is any option to indiciate the
            # display language to the API to retrieve translations in German
            # rather than in English
            if "(Translated by Google)" not in raw_comment:
                extracted['text'] = extracted['text_english'] = raw_comment
                extracted['language'] = "english"
                # NOTE: New reviews might not yet have been translated even if
                # they are not in English.
            elif raw_comment.startswith("(Translated by Google)"):
                # English display language, review was originally in German
                # German display language, review was originally in Russian
                # Review has the format:
                #   (Translated by Google) [English translation]
                #
                #   (Original)
                #   [original text]
                texts = raw_comment[len("(Translated by Google)"):].split(
                    "(Original)")
                extracted['text'] = texts[1].strip()
                extracted['text_english'] = texts[0].strip()
                extracted['language'] = "other"
            else:
                # German display language, review was originally in English
                # English display language, review was originally in German
                # Review has the format:
                #   [original text]
                #
                #   (Translated by Google)
                #   [English translation]
                texts = raw_comment.split("(Translated by Google)")
                extracted['text'] = texts[0].strip()
                extracted['text_english'] = texts[1].strip()
                extracted['language'] = "german"

        return extracted

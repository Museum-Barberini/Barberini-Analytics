import json
import luigi
import os
import pandas as pd
import random
import requests

from itertools import chain

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from museum_facts import MuseumFacts


class GooglePlaystoreReviewsToDb(CsvToDb):

    table = 'gplay_review'

    def requires(self):
        return FetchGplayReviews()


class FetchGplayReviews(DataPreparationTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._app_id = None
        self._url = None

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gplay_reviews.csv',
            format=luigi.format.UTF8
        )

    def run(self):

        print('Fetching Gplay reviews..')
        reviews = self.fetch_all()
        reviews = self.convert_to_right_output_format(reviews)

        print('Saving Gplay reviews')
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False)

    def fetch_all(self):

        # Different languages have different reviews. Iterate over
        # the language codes to fetch all reviews.
        language_codes = self.get_language_codes()
        if self.minimal_mode:
            random_num = random.randint(0, len(language_codes) - 2)
            language_codes = language_codes[random_num:random_num + 2]

        reviews_nested = [
            self.fetch_for_language(language_code)
            for language_code in language_codes
        ]
        reviews_flattened = list(chain.from_iterable(reviews_nested))
        reviews_df = pd.DataFrame(
            reviews_flattened,
            columns=['id', 'date', 'score', 'text',
                     'title', 'thumbsUp', 'version', 'app_id']
        )
        return reviews_df.drop_duplicates()

    def get_language_codes(self):
        language_codes_df = pd.read_csv('src/gplay/language_codes_gplay.csv')
        return language_codes_df['code'].to_list()

    def fetch_for_language(self, language_code):
        """
        Request reviews for a specific language from the webserver that
        serves the gplay api.

        Note: If the language_code is not supported, the gplay api returns
        english reviews.
        """

        response = requests.get(
            url=self.url,
            params={
                'lang': language_code,
                # num: max number of reviews to be fetched. We want all reviews
                'num': 1000000
            }
        )
        # task should fail if request is not successful
        response.raise_for_status()

        reviews = response.json()['results']

        # only return the values we want to keep
        keep_values = ['id', 'date', 'score',
                       'text', 'title', 'thumbsUp', 'version']
        reviews_reduced = [
            {
                **{
                    key: r[key] for key in keep_values
                },
                'app_id': self.app_id
            }
            for r in reviews
        ]

        return reviews_reduced

    @property
    def url(self):
        """
        The webserver that serves the gplay api runs in a different
        container. The container name is user specific:
            [CONTAINER_USER]-gplay-api
        Note that the container name and the CONTAINER_USER
        environment variable are set in the docker-compose.yml.
        """
        if self._url:
            return self._url

        user = os.getenv('CONTAINER_USER')
        self._url = \
            f'http://{user}-gplay-api:3000/api/apps/{self.app_id}/reviews'
        return self._url

    @property
    def app_id(self):
        if self._app_id:
            return self._app_id

        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            self._app_id = facts['ids']['gplay']['appId']
        return self._app_id

    def convert_to_right_output_format(self, reviews):
        """
        Make sure that the review dataframe fits the format expected by
        GooglePlaystoreReviewsToDb. Rename and reorder columns, set data
        types explicitly.
        """

        reviews = reviews.rename(columns={
            'id': 'playstore_review_id',
            'score': 'rating',
            'version': 'app_version',
            'thumbsUp': 'likes'
        })
        columns = {
            'playstore_review_id': str,
            'text': str,
            'rating': int,
            'app_version': str,
            'likes': int,
            'title': str,
            'date': str,
            'app_id': str
        }
        reviews = reviews[columns.keys()]
        reviews = reviews.astype(columns)
        return reviews

import json
import luigi
import os
import pandas as pd
import random
import requests

from itertools import chain

from _utils import CsvToDb, DataPreparationTask, MuseumFacts


class GooglePlaystoreReviewsToDb(CsvToDb):

    table = 'gplay_review'

    def requires(self):

        return FetchGplayReviews()


class FetchGplayReviews(DataPreparationTask):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self._app_id = None
        self._url = None

    column_names = {
        'id': 'playstore_review_id',
        'date': 'date',
        'score': 'rating',
        'text': 'text',
        'thumbsUp': 'likes',
        'version': 'app_version',
        'app_id': 'app_id'
    }

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
        language_codes = self.load_language_codes()
        if self.minimal_mode:
            random_num = random.randint(0, len(language_codes) - 2)
            language_codes = list({
                *language_codes[random_num:random_num + 2],
                'de'  # make sure we do not get zero reviews
            })

        reviews_nested = [
            self.fetch_for_language(language_code)
            for language_code in language_codes
        ]
        reviews_flattened = list(chain.from_iterable(reviews_nested))
        reviews_df = pd.DataFrame(
            reviews_flattened,
            columns=self.column_names.keys()
        )
        return reviews_df.drop_duplicates()

    def load_language_codes(self):

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
                # max number of reviews to be fetched. We want all reviews.
                'num': 1000000 if not self.minimal_mode else 12
            }
        )
        response.raise_for_status()

        reviews = response.json()['results']

        for review in reviews:
            yield {
                **{
                    key: review[key]
                    for key in self.column_names
                    if key != 'app_id'
                },
                'app_id': self.app_id
            }

    @property
    def url(self):
        """
        The webserver that serves the gplay api runs in a different
        container. The container name is user specific:
            [CONTAINER_USER]-barberini_analytics-gplay_api
        Note that the container name and the CONTAINER_USER
        environment variable are set in the docker-compose.yml.
        """

        if self._url:
            return self._url

        user = os.getenv('CONTAINER_USER')
        self._url = (
            f'http://{user}-barberini_analytics-gplay_api:3000/api/apps/'
            f'{self.app_id}/reviews'
        )
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

        reviews = reviews.rename(columns=self.column_names)
        columns = {
            'playstore_review_id': str,
            'text': str,
            'rating': int,
            'app_version': str,
            'likes': int,
            'date': str,
            'app_id': str
        }
        reviews = reviews[columns.keys()]
        reviews = reviews.astype(columns)
        return reviews

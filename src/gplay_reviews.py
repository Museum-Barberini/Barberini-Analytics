import json
import luigi
import os
import pandas as pd
import requests

from itertools import chain

from csv_to_db import CsvToDb
from museum_facts import MuseumFacts


class FetchGplayReviews(luigi.Task):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            'output/gplay_reviews.csv', format=luigi.format.UTF8)

    def run(self):

        print('Fetching Gplay reviews..')
        reviews = self.fetch_all()
        reviews = self.prepare_for_output(reviews)

        print('Saving Gplay reviews')
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False)

    def fetch_all(self):

        # Different languages have different reviews. Iterate over
        # the lanugage codes to fetch all reviews.
        language_codes = self.get_language_codes()
        reviews_nested = [
            self.fetch_for_language(language_code)
            for language_code in language_codes
        ]
        reviews_flattened = list(chain.from_iterable(reviews_nested))

        if len(reviews_flattened) > 0:
            return pd.DataFrame(reviews_flattened).drop_duplicates()
        else:
            # even if no reviews were found the output dataframe should
            # conform to the expected format.
            return pd.DataFrame(
                columns=['id', 'date', 'score', 'text',
                         'title', 'thumbsUp', 'version']
            )

    def get_language_codes(self):
        return pd.read_csv('data/language_codes_gplay.csv')['code'].to_list()

    def fetch_for_language(self, language_code):

        # send a request to the webserver that serves the gplay api
        res = requests.get(
            url=self.get_url(),
            params={
                'lang': language_code,
                'num': 1000000
            }
        )
        # task should fail if request is not successful
        res.raise_for_status()

        res = json.loads(res.text)["results"]

        # only return the values we want to keep
        keep_values = ['id', 'date', 'score',
                       'text', 'title', 'thumbsUp', 'version']
        res_reduced = [{key: r[key] for key in keep_values} for r in res]

        return res_reduced

    def get_url(self):

        # The webserver that serves the gplay api runs in a different
        # container.The container name is user specific:
        # [CONTAINER_USER]-gplay-api
        # Note that the container name and the CONTAINER_USER
        # environment variable are set in the docker-compose.yml
        user = os.getenv('CONTAINER_USER')
        app_id = self.get_app_id()
        return f'http://{user}-gplay-api:3000/api/apps/{app_id}/reviews'

    def get_app_id(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            app_id = facts['ids']['gplay']['appId']
        return app_id

    def prepare_for_output(self, reviews):
        """
        Make sure that the review dataframe fits the format that the
        ToDB-Task expects. Rename and reorder columns, set data
        types explicitly.
        """

        reviews = reviews.rename(columns={
            'id': 'playstore_review_id',
            'score': 'rating',
            'version': 'app_version',
            'thumbsUp': 'thumbs_up'
        })
        reviews = reviews[['playstore_review_id', 'text', 'rating',
                           'app_version', 'thumbs_up', 'title', 'date']]
        reviews = reviews.astype({
            'playstore_review_id': str,
            'text': str,
            'rating': int,
            'app_version': str,
            'thumbs_up': int,
            'title': str,
            'date': str
        })
        return reviews


class GooglePlaystoreReviewsToDB(CsvToDb):

    table = 'gplay_review'

    columns = [
        ('playstore_review_id', 'TEXT'),
        ('text', 'TEXT'),
        ('rating', 'INT'),
        ('app_version', 'TEXT'),
        ('thumbs_up', 'INT'),
        ('title', 'TEXT'),
        ('date', 'TIMESTAMP')
    ]

    primary_key = 'playstore_review_id'

    def requires(self):
        return FetchGplayReviews()

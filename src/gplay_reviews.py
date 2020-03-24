import json
import luigi
import os
import pandas as pd
import requests

from itertools import chain

from museum_facts import MuseumFacts


class FetchGplayReviews(luigi.Task):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget('output/gplay_reviews.csv')

    def run(self):

        print('Fetching Gplay reviews..')
        reviews = fetch_all()

        print('Saving Gplay reviews')
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False)

    def fetch_all(self):
        
        # Different languages have different reviews. Iterate over
        # the lanugage codes to fetch all reviews. 
        language_codes = self.get_language_codes()
        reviews = [
            self.fetch_for_language(language_code)
            for language_code in language_codes
        ]
        reviews_flattened = list(chain.from_iterable(reviews))
        return pd.DataFrame(reviews_flattened).drop_duplicates()

    def get_language_codes(self):
        return pd.read_csv('data/language_codes_gplay.csv')['code'].to_list()

    def fetch_for_language(self, language_code):

        # send a request to the webserver that serves the gplay api
        res = requests.get(
            url = self.get_url(),
            params = {
                'lang': language_code,
                'num': 1000000
            }
        )
        # task should fail if request is not successful
        res.raise_for_status()

        res = json.loads(res.text)["results"]

        # only return the values we want to keep
        keep_values = ['id', 'date', 'score', 'text', 'title', 'thumbsUp', 'version']
        res_reduced = [{key: r[key] for key in keep_values} for r in res]

        return res_reduced

    def get_url(self):

        # The webserver that serves the gplay api runs in a different 
        # container. The container name is user specific: [CONTAINER_USER]-gplay-api
        # Note that the container name is set in the docker-compose.yml
        user = os.getenv('CONTAINER_USER')
        app_id = self.get_app_id()
        return f'http://{user}-gplay-api:3000/api/apps/{app_id}/reviews'

    def get_app_id(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            app_id = facts['ids']['gplay']['appId']
        return app_id

import json
import luigi
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
        
        # the reviews only change by language, not by country code
        language_codes = self.get_language_codes()
        reviews = [
            self.fetch_for_language(language_code)
            for language_code in language_codes
        ]
        reviews_flattened = list(chain.from_iterable(res))
        return pd.DataFrame(reviews_flattened).drop_duplicates(subset = ["id"])

    def get_language_codes(self):
        return pd.read_csv('data/language_codes_gplay.csv')['code'].to_list()

    def fetch_for_language(self, language_code):

        res = requests.get(
            url = f"http://localhost:3000/api/apps/{self.get_app_id()}/reviews",
            params = {
                "lang": lang_code,
                "num": 10000
            }
        )
        return json.loads(res.text)["results"]

    def get_app_id(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            app_id = facts['ids']['gplay']['appId']
        return app_id

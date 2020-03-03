import json

import luigi
import pandas as pd
import requests
import xmltodict
from luigi.format import UTF8

from csv_to_db import CsvToDb
from museum_facts import MuseumFacts


class FetchAppstoreReviews(luigi.Task):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget('output/appstore_reviews.csv', format=UTF8)

    def run(self):
        reviews = self.fetch_all()
        print("storing results")
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False, header=True)

    def fetch_all(self):
        data = []
        country_codes = self.get_country_codes()
        print()
        try:
            for index, country_code in enumerate(country_codes, start=1):
                try:
                    data.append(self.fetch_for_country(country_code))
                except ValueError:
                    pass  # no data for given country code
                except requests.HTTPError as error:
                    if error.response.status_code == 400:
                        # not all countries are available
                        pass
                    else:
                        raise
                print(
                    f"\rFetched appstore reviews for {country_code} "
                    f"({100. * index / len(country_codes)}%)",
                    end='',
                    flush=True)
        finally:
            print()
        ret = pd.concat(data)
        return ret.drop_duplicates(subset=['appstore_review_id'])

    def get_country_codes(self):
        return requests.get('http://country.io/names.json').json().keys()

    def fetch_for_country(self, country_code):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            app_id = facts['ids']['apple']['appId']
        url = (f'https://itunes.apple.com/{country_code}/rss/customerreviews/'
               f'page=1/id={app_id}/sortby=mostrecent/xml')
        data_list = []

        while url is not None:
            data, url = self.fetch_page(url)
            data_list += data

        if len(data_list) == 0:
            # no reviews for the given country code
            raise ValueError()

        result = pd.DataFrame(data_list)
        result['country_code'] = country_code

        return result

    def fetch_page(self, url):

        response = requests.get(url)
        response.raise_for_status()
        response_content = xmltodict.parse(response.text)['feed']

        if 'entry' not in response_content:
            return [], None

        entries = response_content['entry']

        if isinstance(entries, dict):
            entries = [entries]
        data = [{'appstore_review_id': item['id'],
                 'text': self.find_first_conditional_tag(
                     item['content'],
                     lambda each: each['@type'] == 'text')['#text'],
                 'rating': item['im:rating'],
                 'app_version': item['im:version'],
                 'vote_count': item['im:voteCount'],
                 'vote_sum': item['im:voteSum'],
                 'title': item['title'],
                 'date': item['updated']} for item in entries]

        # read <link rel="next"> which contains the link to the next page
        next_page_url = self.find_first_conditional_tag(
            response_content['link'],
            lambda each: each['@rel'] == 'next')['@href']

        return data, next_page_url

    # for when there are multiple 'contents'-elements in our response
    def find_first_conditional_tag(self, tags, condition):
        return next(each for each in tags if condition(each))


class AppstoreReviewsToDB(CsvToDb):

    table = 'appstore_review'

    columns = [
        ('appstore_review_id', 'TEXT'),
        ('text', 'TEXT'),
        ('rating', 'INT'),
        ('app_version', 'TEXT'),
        ('vote_count', 'INT'),
        ('vote_sum', 'INT'),
        ('title', 'TEXT'),
        ('date', 'TIMESTAMP'),
        ('country_code', 'TEXT')
    ]

    primary_key = 'appstore_review_id'

    def requires(self):
        return FetchAppstoreReviews()

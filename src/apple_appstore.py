"""Provides tasks for downloading all Apple App Store reviews about the app."""

import json

import luigi
import pandas as pd
import random
import requests
import xmltodict
from luigi.format import UTF8

from _utils import CsvToDb, DataPreparationTask, MuseumFacts, logger


class AppstoreReviewsToDb(CsvToDb):
    """Store all download App Store reviews into the database."""

    table = 'appstore_review'

    def requires(self):
        return FetchAppstoreReviews()


class FetchAppstoreReviews(DataPreparationTask):
    """
    Download all reviews related to the museum app from the Apple App Store.

    The data is accessed by scanning an RSS feed.
    """

    table = 'appstore_review'

    def requires(self):

        return MuseumFacts()

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/appstore_reviews.csv', format=UTF8)

    def run(self):

        reviews = self.fetch_all()
        logger.info("storing results")
        with self.output().open('w') as output_file:
            reviews.to_csv(output_file, index=False, header=True)

    def fetch_all(self):

        data = []
        country_codes = sorted(self.get_country_codes())
        if self.minimal_mode:
            random_num = random.randint(0, len(country_codes) - 2)
            country_codes = country_codes[random_num:random_num + 2]
            country_codes.append('CA')

        tbar = self.tqdm(country_codes, desc="Fetching appstore reviews")
        for country_code in tbar:
            tbar.set_description(
                f"Fetching appstore reviews ({country_code})"
            )
            try:
                data_for_country = self.fetch_for_country(country_code)
                if not data_for_country.empty:
                    data.append(data_for_country)
                logger.debug(f'Fetching appstore reviews for {country_code}')
            except requests.HTTPError as error:
                if error.response.status_code == 400:
                    # not all countries are available
                    pass
                else:
                    raise
        try:
            ret = pd.concat(data)
        except ValueError:
            ret = pd.DataFrame(columns=[])

        return ret.drop_duplicates(subset=['app_id', 'appstore_review_id'])

    def get_country_codes(self):

        return requests.get('http://country.io/names.json').json().keys()

    def fetch_for_country(self, country_code):

        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
            app_id = facts['ids']['apple']['appId']
        url = (f'https://itunes.apple.com/{country_code}/rss/customerreviews/'
               f'page=1/id={app_id}/sortby=mostrecent/xml')
        data_list = []

        while url:
            try:
                data, url = self.fetch_page(url)
                data_list += data
            except requests.exceptions.HTTPError as error:
                if error.response is not None and (
                    error.response.status_code == 503 or (
                        error.response.status_code in {403, 404}
                        and country_code not in {'DE', 'US', 'GB'})):
                    logger.error(f"Encountered {error.response.status_code} "
                                 f"server error '{error}' for country code "
                                 f"'{country_code}'")
                    logger.error("Continuing anyway...")
                    break
                else:
                    raise

        if not data_list:
            # no reviews for the given country code
            logger.debug(f"Empty data for country {country_code}")

        result = pd.DataFrame(data_list)
        result['country_code'] = country_code
        result.insert(0, 'app_id', app_id)

        return result

    def fetch_page(self, url):

        response = requests.get(url)
        response.raise_for_status()
        # specify encoding explicitly because the autodetection fails sometimes
        response.encoding = 'utf-8'
        response_content = xmltodict.parse(response.text)['feed']

        if 'entry' not in response_content:
            return [], None

        entries = response_content['entry']

        if isinstance(entries, dict):
            entries = [entries]
        data = [
            {
                'appstore_review_id': item['id'],
                'text': self.find_first_conditional_tag(
                    item['content'],
                    lambda each: each['@type'] == 'text')['#text'],
                'rating': item['im:rating'],
                'app_version': item['im:version'],
                'vote_count': item['im:voteCount'],
                'vote_sum': item['im:voteSum'],
                'title': item['title'],
                'date': item['updated']
            }
            for item in entries
        ]

        # read <link rel="next"> which contains the link to the next page
        next_page_url = self.find_first_conditional_tag(
            response_content['link'],
            lambda each: each['@rel'] == 'next')['@href']

        return data, next_page_url

    # for when there are multiple 'contents'-elements in our response
    def find_first_conditional_tag(self, tags, condition):

        return next(each for each in tags if condition(each))

import datetime as dt
import json
import logging
import os

import luigi
import pandas as pd
import requests
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from museum_facts import MuseumFacts
from set_db_connection_options import set_db_connection_options

logger = logging.getLogger('luigi-interface')


class FbPostsToDB(CsvToDb):
    minimal = luigi.parameter.BoolParameter(default=False)

    table = 'fb_post'

    columns = [
        ('post_date', 'TIMESTAMP'),
        ('text', 'TEXT'),
        ('fb_post_id', 'TEXT')
    ]

    primary_key = 'fb_post_id'

    def requires(self):
        return FetchFbPosts(minimal=self.minimal)


class FbPostPerformanceToDB(CsvToDb):
    minimal = luigi.parameter.BoolParameter(default=False)

    table = 'fb_post_performance'

    columns = [
        ('fb_post_id', 'TEXT'),
        ('time_stamp', 'TIMESTAMP'),
        ('react_like', 'INT'),
        ('react_love', 'INT'),
        ('react_wow', 'INT'),
        ('react_haha', 'INT'),
        ('react_sorry', 'INT'),
        ('react_anger', 'INT'),
        ('likes', 'INT'),
        ('shares', 'INT'),
        ('comments', 'INT'),
        ('video_clicks', 'INT'),
        ('link_clicks', 'INT'),
        ('other_clicks', 'INT'),
        ('negative_feedback', 'INT'),
        ('paid_impressions', 'INT')
    ]

    primary_key = ('fb_post_id', 'time_stamp')

    foreign_keys = [
            {
                'origin_column': 'fb_post_id',
                'target_table': 'fb_post',
                'target_column': 'fb_post_id'
            }
        ]

    def requires(self):
        return FetchFbPostPerformance(
            foreign_keys=self.foreign_keys,
            minimal=self.minimal)


class FetchFbPosts(DataPreparationTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget('output/facebook/fb_posts.csv', format=UTF8)

    def run(self):
        access_token = os.environ['FB_ACCESS_TOKEN']
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['facebook']['pageId']

        posts = []

        url = f'https://graph.facebook.com/v6.0/{page_id}/feed'
        headers = {'Authorization': 'Bearer ' + access_token}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        response_content = response.json()
        for post in (response_content['data']):
            posts.append(post)

        logger.info("Fetching facebook posts ...")
        page_count = 0
        while ('next' in response_content['paging']):
            page_count = page_count + 1
            url = response_content['paging']['next']
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            response_content = response.json()
            for post in (response_content['data']):
                posts.append(post)
            print(f"\rFetched facebook page {page_count}", end='', flush=True)

            if self.minimal:
                response_content.pop('next')

        logger.info("Fetching of facebook posts completed")

        with self.output().open('w') as output_file:
            df = pd.DataFrame([post for post in posts])
            df = df.filter(['created_time', 'message', 'id'])
            df.columns = ['post_date', 'text', 'fb_post_id']
            df.to_csv(output_file, index=False, header=True)


class FetchFbPostPerformance(DataPreparationTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    # Override the default timeout of 10 minutes to allow
    # FetchFbPostPerformance to take up to 20 minutes.
    worker_timeout = 1200

    def _requires(self):
        return luigi.task.flatten([
            FbPostsToDB(minimal=self.minimal),
            super()._requires()
        ])

    def requires(self):
        return FetchFbPosts(minimal=self.minimal)

    def output(self):
        return luigi.LocalTarget(
            'output/facebook/fb_post_performances.csv', format=UTF8)

    def run(self):
        access_token = os.environ['FB_ACCESS_TOKEN']

        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        performances = []
        with self.input().open('r') as csv_in:
            df = pd.read_csv(csv_in)

        if self.minimal:
            df = df.head(5)

        invalid_count = 0
        for index in df.index:
            post_id = df['fb_post_id'][index]
            url = f'https://graph.facebook.com/v6.0/{post_id}/insights'
            metrics = [
                'post_reactions_by_type_total',
                'post_activity_by_action_type',
                'post_clicks_by_type',
                'post_negative_feedback',
                'post_impressions_paid'
            ]
            request_args = {
                'params': {'metric': ','.join(metrics)},
                'headers': {'Authorization': 'Bearer ' + access_token}
            }

            response = self.try_request_multiple_times(url, request_args)

            if response.status_code == 400:
                invalid_count += 1
                continue

            response.raise_for_status()  # in case of another error

            response_content = response.json()

            post_perf = dict()
            post_perf['fb_post_id'] = post_id
            post_perf['time_stamp'] = current_timestamp

            # Reactions
            reactions = response_content['data'][0]['values'][0]['value']
            post_perf['react_like'] = int(reactions.get('like', 0))
            post_perf['react_love'] = int(reactions.get('love', 0))
            post_perf['react_wow'] = int(reactions.get('wow', 0))
            post_perf['react_haha'] = int(reactions.get('haha', 0))
            post_perf['react_sorry'] = int(reactions.get('sorry', 0))
            post_perf['react_anger'] = int(reactions.get('anger', 0))

            # Activity
            activity = response_content['data'][1]['values'][0]['value']
            post_perf['likes'] = int(activity.get('LIKE', 0))
            post_perf['shares'] = int(activity.get('SHARE', 0))
            post_perf['comments'] = int(activity.get('COMMENT', 0))

            # Clicks
            clicks = response_content['data'][2]['values'][0]['value']
            post_perf['video_clicks'] = int(clicks.get('video play', 0))
            post_perf['link_clicks'] = int(clicks.get('link clicks', 0))
            post_perf['other_clicks'] = int(clicks.get('other clicks', 0))

            # negative feedback (only one field)
            post_perf['negative_feedback'] = \
                response_content['data'][3]['values'][0]['value']

            # number of times the post entered a person's screen through
            # paid distribution such as an ad
            post_perf['paid_impressions'] = \
                response_content['data'][4]['values'][0]['value']

            performances.append(post_perf)

        df = self.ensure_foreign_keys(df)
        if invalid_count:
            logger.warning(f"Skipped {invalid_count} posts")

        with self.output().open('w') as output_file:
            df = pd.DataFrame([perf for perf in performances])
            df.to_csv(output_file, index=False, header=True)

    def try_request_multiple_times(self, url, request_args):
        """
        Not all requests to the facebook api are successful. To allow
        some requests to fail (mainly: to time out), request the api up
        to four times.
        """
        for _ in range(3):
            try:
                response = requests.get(url, request_args, timeout=60)
                if response.ok:
                    # If response is not okay, we usually get a 400 status code
                    return response
            except Exception as e:
                print(
                    "An Error occured requesting the Facebook api.\n"
                    "Trying to request the api again.\n"
                    f"error message: {e}"
                )
        return requests.get(url, request_args, timeout=100)

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

logger = logging.getLogger('luigi-interface')

API_VER = 'v6.0'
API_BASE = f'https://graph.facebook.com/{API_VER}'


class FbPostsToDB(CsvToDb):

    table = 'fb_post'

    columns = [
        ('post_date', 'TIMESTAMP'),
        ('text', 'TEXT'),
        ('fb_post_id', 'TEXT')
    ]

    primary_key = 'fb_post_id'

    def requires(self):
        return FetchFbPosts()


class FbPostPerformanceToDB(CsvToDb):

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
        return FetchFbPostPerformance(foreign_keys=self.foreign_keys)


class FetchFbPosts(DataPreparationTask):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget('output/facebook/fb_posts.csv', format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['facebook']['pageId']

        posts = []

        url = f'{API_BASE}/{page_id}/feed'

        response = try_request_multiple_times(url)
        response.raise_for_status()

        response_content = response.json()
        for post in (response_content['data']):
            posts.append(post)

        logger.info("Fetching facebook posts ...")
        page_count = 0
        while ('next' in response_content['paging']):
            page_count = page_count + 1
            url = response_content['paging']['next']
            response = try_request_multiple_times(url)
            response.raise_for_status()

            response_content = response.json()
            for post in (response_content['data']):
                posts.append(post)
            print(f"\rFetched facebook page {page_count}", end='', flush=True)

            if os.environ['MINIMAL'] == 'True':
                response_content['paging'].pop('next')

        logger.info("Fetching of facebook posts completed")

        with self.output().open('w') as output_file:
            df = pd.DataFrame([post for post in posts])
            df = df.filter(['created_time', 'message', 'id'])
            df.columns = ['post_date', 'text', 'fb_post_id']
            df.to_csv(output_file, index=False, header=True)


class FetchFbPostPerformance(DataPreparationTask):
    timespan = luigi.parameter.TimeDeltaParameter(
        default=dt.timedelta(days=60),
        description="For how much time posts should be fetched")

    # Override the default timeout of 10 minutes to allow
    # FetchFbPostPerformance to take up to 20 minutes.
    worker_timeout = 1200

    def _requires(self):
        return luigi.task.flatten([
            FbPostsToDB(),
            super()._requires()
        ])

    def requires(self):
        return FetchFbPosts()

    def output(self):
        return luigi.LocalTarget(
            'output/facebook/fb_post_performances.csv', format=UTF8)

    def run(self):
        access_token = os.getenv('FB_ACCESS_TOKEN')

        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        earliest_valid_date = (dt.datetime.now()
                               - self.timespan)
        performances = []
        with self.input().open('r') as csv_in:
            df = pd.read_csv(csv_in)

        if os.environ['MINIMAL'] == 'True':
            df = df.head(5)

        invalid_count = 0
        for index in df.index:
            post_id = df['fb_post_id'][index]
            post_date = dt.datetime.strptime(
                df['post_date'][index],
                '%Y-%m-%dT%H:%M:%S+%f')
            if post_date < earliest_valid_date:
                continue

            # print(f"[FB] Loading performance data for post {str(post_id)}")
            url = f'{API_BASE}/{post_id}/insights'
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

            response = try_request_multiple_times(url, **request_args)

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

        if invalid_count:
            logger.warning(f"Skipped {invalid_count} posts")

        df = pd.DataFrame([perf for perf in performances])
        df, _ = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


def try_request_multiple_times(url, **kwargs):
    """
    Not all requests to the facebook api are successful. To allow
    some requests to fail (mainly: to time out), request the api up
    to four times.
    """
    headers = kwargs.pop('headers', None)
    if not headers:
        access_token = os.getenv('FB_ACCESS_TOKEN')
        if not access_token:
            raise EnvironmentError("FB Access token is not set")
        headers = {'Authorization': 'Bearer ' + access_token}

    for _ in range(3):
        try:
            response = requests.get(
                url,
                timeout=60,
                headers=headers,
                **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(
                "An Error occured requesting the Facebook api.\n"
                "Trying to request the api again.\n"
                f"error message: {e}"
            )
    return requests.get(url, timeout=100, **kwargs)

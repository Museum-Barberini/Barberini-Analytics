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

    def requires(self):
        return FetchFbPosts()


class FbPostPerformanceToDB(CsvToDb):

    table = 'fb_post_performance'

    def requires(self):
        return FetchFbPostPerformance(table=self.table)


class FetchFbPosts(DataPreparationTask):

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/facebook/fb_posts.csv', format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['facebook']['pageId']

        posts = self.fetch_posts(page_id)
        df = self.transform_posts(posts)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def fetch_posts(self, page_id):
        url = f'{API_BASE}/{page_id}/published_posts?limit=100'

        response = try_request_multiple_times(url)
        response_content = response.json()
        yield from response_content['data']

        log_loop = self.loop_verbose(msg="Fetching facebook page {index}")
        while 'next' in response_content['paging']:
            url = response_content['paging']['next']
            next(log_loop)
            response = try_request_multiple_times(url)
            response_content = response.json()
            yield from response_content['data']

            if self.minimal_mode:
                response_content['paging'].pop('next')

        logger.info("Fetching of facebook posts completed")

    def transform_posts(self, posts):
        df = pd.DataFrame(posts)
        fb_post_ids = df['id'].str.split('_', n=1, expand=True)
        df = df.filter(['created_time', 'message'])
        df = fb_post_ids.join(df)
        df.columns = ['page_id', 'post_id', 'post_date', 'text']
        return df


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
            f'{self.output_dir}/facebook/fb_post_performances.csv',
            format=UTF8
        )

    def run(self):
        access_token = os.getenv('FB_ACCESS_TOKEN')

        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        minimum_relevant_date = dt.datetime.now() - self.timespan
        performances = []
        with self.input().open('r') as csv_in:
            df = pd.read_csv(csv_in)

        if self.minimal_mode:
            df = df.head(5)

        invalid_count = 0
        for index in self.iter_verbose(
                df.index,
                msg="Fetching performance data for FB post {index}/{size}"):
            page_id, post_id = df['page_id'][index], df['post_id'][index]
            fb_post_id = f'{page_id}_{post_id}'
            post_date = dt.datetime.strptime(
                df['post_date'][index],
                '%Y-%m-%dT%H:%M:%S+%f')
            if post_date < minimum_relevant_date:
                continue

            logger.debug(
                f"Loading performance data for FB post {fb_post_id}")

            url = f'{API_BASE}/{fb_post_id}/insights'
            metrics = [
                'post_reactions_by_type_total',
                'post_activity_by_action_type',
                'post_clicks_by_type',
                'post_negative_feedback',
                'post_impressions_paid',
                'post_impressions',
                'post_impressions_unique'  # "reach"
            ]

            response = try_request_multiple_times(
                url,
                params={'metric': ','.join(metrics)},
                headers={'Authorization': 'Bearer ' + access_token}
            )
            if response.status_code == 400:
                invalid_count += 1
                continue
            response.raise_for_status()  # in case of another error
            response_content = response.json()

            post_perf = {
                'time_stamp': current_timestamp,
            }

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
            post_perf['likes'] = int(activity.get('like', 0))
            post_perf['shares'] = int(activity.get('share', 0))
            post_perf['comments'] = int(activity.get('comment', 0))

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

            post_perf['post_impressions'] = \
                response_content['data'][5]['values'][0]['value']

            post_perf['post_impressions_unique'] = \
                response_content['data'][6]['values'][0]['value']

            post_perf.update(
                page_id=page_id,
                post_id=post_id
            )
            performances.append(post_perf)
        if invalid_count:
            logger.warning(f"Skipped {invalid_count} posts")

        df = pd.DataFrame(performances)
        df = self.ensure_foreign_keys(df)

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
                "An Error occurred requesting the Facebook API.\n"
                "Trying to request the API again.\n"
                f"Error message: {e}"
            )
    response = requests.get(url, timeout=100, headers=headers, **kwargs)

    # cause clear error instead of trying
    # to process the invalid response
    response.raise_for_status()
    return response

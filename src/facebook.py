"""
Provides tasks and utilities for fetching Facebook posts and linked details.

All data are fetched using the Facebook Graph API. An access token has to be
available in the /etc/secrets/keys.env file.
"""

import datetime as dt
import json
import os

import luigi
import numpy as np
import pandas as pd
import requests
from luigi.format import UTF8

from _utils import CsvToDb, DataPreparationTask, MuseumFacts, logger

API_VER = 'v17.0'
API_BASE = f'https://graph.facebook.com/{API_VER}'

# ======= ToDb Tasks =======


class FbPostsToDb(CsvToDb):
    """Store all fetched Facebook posts into the database."""

    table = 'fb_post'

    def requires(self):

        return FetchFbPosts()


class FbPostPerformanceToDb(CsvToDb):
    """Store all fetched post performance data into the database."""

    table = 'fb_post_performance'

    def requires(self):

        return FetchFbPostPerformance(table=self.table)


class FbPostCommentsToDb(CsvToDb):
    """Store all fetched Facebook comments into the database."""

    table = 'fb_post_comment'

    def requires(self):

        return FetchFbPostComments(table=self.table)

# ======= FetchTasks =======


class FetchFbPosts(DataPreparationTask):
    """Fetch all Facebook post from the museum's profile page."""

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

        limit = 100
        url = f'{API_BASE}/{page_id}/published_posts?limit={limit}'

        response = try_request_multiple_times(url)
        response_content = response.json()
        yield from response_content['data']

        i = 1
        while 'paging' in response_content and \
                'next' in response_content['paging']:
            logger.info(f"Fetched approx. {i * limit} Facebook posts")
            i += 1
            url = response_content['paging']['next']

            try:
                response = try_request_multiple_times(url)
            except requests.exceptions.RequestException as e:
                if e.response.json()['error']['code'] != 1:
                    raise
                # "Please reduce the amount of data you're asking for, then
                # retry your request"
                logger.warning("Facebook API internal limit hit, won't fetch "
                               "more posts")
                # https://developers.facebook.com/support/bugs/1147688208992941/
                break
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


class FetchFbPostDetails(DataPreparationTask):
    """The abstract superclass for tasks fetching post-related information."""

    timespan = luigi.parameter.TimeDeltaParameter(
        default=dt.timedelta(days=60),
        description="For how much time posts should be fetched")

    # Override the default timeout of 10 minutes to allow
    # FetchFbPostPerformance to take up to 20 minutes.
    worker_timeout = 1200

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.minimum_relevant_date = dt.datetime.now() - self.timespan

    def _requires(self):

        return luigi.task.flatten([
            FbPostsToDb(),
            super()._requires()
        ])

    def requires(self):

        return FetchFbPosts()

    @staticmethod
    def post_date(df, index):

        return dt.datetime.strptime(
            df['post_date'][index],
            '%Y-%m-%dT%H:%M:%S+%f'
        )


class FetchFbPostPerformance(FetchFbPostDetails):
    """
    Fetch performance data for every Facebook comment.

    Performance data include the number of likes, shares, and comments for a
    post.
    """

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/facebook/fb_post_performances.csv',
            format=UTF8)

    def run(self):

        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        performances = []
        with self.input().open('r') as csv_in:
            df = pd.read_csv(csv_in)

        if self.minimal_mode:
            df = df.head(5)

        invalid_count = 0
        pbar = self.tqdm(
            df.index,
            desc="Fetching performance data for facebook posts"
        )
        for index in pbar:
            page_id, post_id = \
                str(df['page_id'][index]), str(df['post_id'][index])
            fb_post_id = f'{page_id}_{post_id}'
            post_date = self.post_date(df, index)
            if post_date < self.minimum_relevant_date:
                continue

            logger.debug(
                f"Loading performance data for FB post {fb_post_id}")

            metrics = ','.join([
                'post_reactions_by_type_total',
                'post_activity_by_action_type',
                'post_clicks_by_type',
                'post_negative_feedback',
                'post_impressions_paid',
                'post_impressions',
                'post_impressions_unique'  # "reach"
            ])
            url = f'{API_BASE}/{fb_post_id}/insights?metric={metrics}'

            response = try_request_multiple_times(url)

            if response.status_code == 400:
                invalid_count += 1
                continue
            response.raise_for_status()  # in case of another error
            response_content = response.json()

            post_perf = {
                'timestamp': current_timestamp,
            }

            # Reactions
            try:
                reactions = [data for data in response_content['data'] if data['name'] == 'post_reactions_by_type_total'][0]['values'][0]['value']
                post_perf['react_like'] = int(reactions.get('like', 0))
                post_perf['react_love'] = int(reactions.get('love', 0))
                post_perf['react_wow'] = int(reactions.get('wow', 0))
                post_perf['react_haha'] = int(reactions.get('haha', 0))
                post_perf['react_sorry'] = int(reactions.get('sorry', 0))
                post_perf['react_anger'] = int(reactions.get('anger', 0))
            except IndexError:
                pass

            # Activity
            try:
                activity = [data for data in response_content['data'] if data['name'] == 'post_activity_by_action_type'][0]['values'][0]['value']
                post_perf['likes'] = int(activity.get('like', 0))
                post_perf['shares'] = int(activity.get('share', 0))
                post_perf['comments'] = int(activity.get('comment', 0))
            except IndexError:
                pass

            # Clicks
            try:
                clicks = [data for data in response_content['data'] if data['name'] == 'post_clicks_by_type'][0]['values'][0]['value']
                post_perf['video_clicks'] = int(clicks.get('video play', 0))
                post_perf['link_clicks'] = int(clicks.get('link clicks', 0))
                post_perf['other_clicks'] = int(clicks.get('other clicks', 0))
            except IndexError:
                pass

            # negative feedback (only one field)
            try:
                negative_feedback = [data for data in response_content['data'] if data['name'] == 'post_negative_feedback'][0]['values'][0]['value']
                post_perf['negative_feedback'] = int(negative_feedback)
            except IndexError:
                pass

            # number of times the post entered a person's screen through
            # paid distribution such as an ad
            try:
                paid_impressions = [data for data in response_content['data'] if data['name'] == 'post_impressions_paid'][0]['values'][0]['value']
                post_perf['paid_impressions'] = int(paid_impressions)
            except IndexError:
                pass

            try:
                impressions = [data for data in response_content['data'] if data['name'] == 'post_impressions'][0]['values'][0]['value']
                post_perf['post_impressions'] = int(impressions)
            except IndexError:
                pass

            try:
                impressions_unique = [data for data in response_content['data'] if data['name'] == 'post_impressions_unique'][0]['values'][0]['value']
                post_perf['post_impressions_unique'] = int(impressions_unique)
            except IndexError:
                pass

            post_perf.update(
                page_id=page_id,
                post_id=post_id
            )
            performances.append(post_perf)
        if invalid_count:
            logger.warning(f"Skipped {invalid_count} posts")

        df = pd.DataFrame(performances)

        # For some reason, all except the first set of performance
        # values get inserted twice into the performances list.
        # Investigate and fix the root cause, this is a workaround
        # TODO: Is this still up to date? Could not reproduce.
        df.drop_duplicates(subset='post_id', inplace=True, ignore_index=True)

        df = self.filter_fkey_violations(df)
        df = self.condense_performance_values(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class FetchFbPostComments(FetchFbPostDetails):
    """Fetch all user comments to Facebook posts."""

    def requires(self):

        yield super().requires()
        yield MuseumFacts()

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/facebook/fb_post_comments.csv',
            format=UTF8)

    def run(self):

        input_ = list(self.input())
        with input_[0].open() as fb_post_file:
            df = pd.read_csv(fb_post_file)
        with input_[1].open() as facts_file:
            self.facts = json.load(facts_file)
        comments = []

        if self.minimal_mode:
            df = df.head(15)

        comments = self.fetch_comments(df)
        df = pd.DataFrame(comments)

        if not df.empty:
            # Posts can appear multiple times, causing comments to
            # be fetched multiple times as well, causing
            # primary key violations
            # See #227
            df = df.drop_duplicates(
                subset=['comment_id', 'post_id'], ignore_index=True)
            df = df.astype({
                'post_id': str,
                'comment_id': str,
                'page_id': str
            })

            df['response_to'] = df['response_to'].apply(_num_to_str)

            df = self.filter_fkey_violations(df)

        else:
            """
            This whole else block is a dirty workaround, because the ToDb tasks
            currently cannot deal with completely empty CSV files as input,
            they assume that at least the header row exists. See #268.
            """
            df = pd.DataFrame(columns=[
                'post_id',
                'page_id',
                'comment_id',
                'text',
                'post_date',
                'is_from_museum',
                'response_to'
            ])

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def fetch_comments(self, df):

        invalid_count = 0

        # Handle each post
        for i in df.index:
            page_id, post_id = df['page_id'][i], df['post_id'][i]
            fb_post_id = f'{page_id}_{post_id}'
            post_date = self.post_date(df, i)
            if post_date < self.minimum_relevant_date:
                continue

            # Grab up to 100 comments for the post (maximum)
            limit = 100

            # 'toplevel' or 'stream' (toplevel doesn't include replies)
            # Using 'toplevel' here allows us to safely
            # set parent to None for all comments returned
            # by the first query
            filt = 'toplevel'

            # 'chronological' or 'reverse_chronolocial'
            order = 'chronological'

            fields = ','.join([
                'id',
                'created_time',
                'comment_count',
                'message',
                'comments'
                ])

            url = (f'{API_BASE}/{fb_post_id}/comments?limit={limit}'
                   f'filter={filt}&order={order}&fields={fields}')

            response = try_request_multiple_times(url)
            if response.status_code == 400:
                invalid_count += 1
                continue
            response_data = response.json().get('data')

            logger.info(f"Fetched {len(response_data)} "
                        f"comments for post {post_id}")

            # Handle each comment for the post
            for comment in response_data:
                comment_id = comment.get('id').split('_')[1]

                yield {
                    'post_id': str(post_id),
                    'comment_id': str(comment_id),
                    'page_id': str(page_id),
                    'post_date': comment.get('created_time'),
                    'text': comment.get('message'),
                    'is_from_museum': self.is_from_museum(comment),
                    'response_to': None
                }

                if not comment.get('comment_count'):
                    continue
                try:
                    # Handle each reply for the comment
                    for reply in comment['comments']['data']:
                        yield {
                            'comment_id': reply.get('id').split('_')[1],
                            'page_id': str(page_id),
                            'post_id': str(post_id),
                            'post_date': reply.get('created_time'),
                            'text': reply.get('message'),
                            'is_from_museum': self.is_from_museum(reply),
                            'response_to': str(comment_id)
                        }
                except KeyError:
                    # Sometimes, replies become unavailable. In this case,
                    # the Graph API returns the true 'comment_count',
                    # but does not provide a 'comments' field anyway
                    logger.warning(
                        f"Failed to retrieve replies for comment "
                        f"{comment.get('id')}")

        if invalid_count:
            logger.warning(f"Skipped {invalid_count} posts")

    def is_from_museum(self, comment_json):

        return comment_json.get('from', {}).get('name') == self.facts['name']


def try_request_multiple_times(url, **kwargs) -> requests.Response:
    """
    Try multiple request to the Facebook Graph API.

    Not all requests to the API are successful. To allow some requests to fail
    (mainly: to time out), request the API up to four times.
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
                f"Error message: {e}\n"
                f"Content: {e.response.json()}\n"
                "Trying to request the API again."
            )
    response = requests.get(url, timeout=100, headers=headers, **kwargs)

    # cause clear error instead of trying
    # to process the invalid response
    # (except if we tried to access a foreign object)
    if not response.ok and not response.status_code == 400:
        response.raise_for_status()
    return response


def _num_to_str(num):
    """
    Convert number from pandas into CSV-stable representation.

    By default, pandas treats columns that contain both integers and None
    values as np.floats. However, we don't want either of this here, we just
    want strings, so convert 'em all!
    """
    if not num:
        return None
    if isinstance(num, str):
        return num
    if np.isnan(num):
        return None
    return str(int(num))

"""
Provides tasks for downloading all Instagram-related data into the database.

All data are fetched using the Facebook Graph API. The credentials are shared
with the access token used in the facebook module. See documentation there.
"""

import datetime as dt
import json
import os
import sys

from dateutil import parser as dtparser
import luigi
import pandas as pd
from luigi.format import UTF8

from _utils import CsvToDb, DataPreparationTask, MuseumFacts, logger
from _utils.data_preparation import PerformanceValueCondenser
from facebook import API_BASE, try_request_multiple_times


# =============== Database Tasks ===============


class IgToDb(luigi.WrapperTask):
    """Download all Instagram-related data into the database."""

    def requires(self):
        yield IgPostsToDb()
        yield IgProfileMetricsDevelopmentToDb()
        yield IgTotalProfileMetricsToDb()
        yield IgAudienceGenderAgeToDb()
        yield IgAudienceCityToDb()
        yield IgAudienceCountryToDb()


class IgPostPerformanceToDb(CsvToDb):
    """Store all fetched performance values Instagram posts into the DB."""

    table = 'ig_post_performance'

    def requires(self):
        return FetchIgPostPerformance(
            table=self.table, columns=[col[0] for col in self.columns])


class IgPostsToDb(CsvToDb):
    """Download all fetched Instagram posts into the database."""

    table = 'ig_post'

    def requires(self):
        return FetchIgPosts()


class IgProfileMetricsDevelopmentToDb(CsvToDb):
    """Store all fetched development values for profile metrics to the DB."""

    table = 'ig_profile_metrics_development'

    def requires(self):
        return FetchIgProfileMetricsDevelopment(
            table=self.table, columns=[col[0] for col in self.columns])


class IgTotalProfileMetricsToDb(CsvToDb):
    """Store all fetched total profile metric values into the database."""

    table = 'ig_total_profile_metrics'

    def requires(self):
        return FetchIgTotalProfileMetrics(
            table=self.table, columns=[col[0] for col in self.columns])


class IgAudienceCityToDb(CsvToDb):
    """Store all fetched audience city values into the database."""

    table = 'ig_audience_city'

    def requires(self):
        return FetchIgAudienceOrigin(
            table=self.table,
            columns=[col[0] for col in self.columns],
            country_mode=False)


class IgAudienceCountryToDb(CsvToDb):
    """Store all fetched audience country values into the database."""

    table = 'ig_audience_country'

    def requires(self):
        return FetchIgAudienceOrigin(
            table=self.table,
            columns=[col[0] for col in self.columns],
            country_mode=True)


class IgAudienceGenderAgeToDb(CsvToDb):
    """Store all fetched audience age values into the database."""

    table = 'ig_audience_gender_age'

    def requires(self):
        return FetchIgAudienceGenderAge(
            table=self.table, columns=[col[0] for col in self.columns])

# =============== DataPreparation Tasks ===============


class FetchIgPosts(DataPreparationTask):
    """Request all instagram posts from the Facebook Graph API."""

    columns = {
        'id': str,
        'caption': str,
        'timestamp': dtparser.parse,
        'media_type': str,
        'like_count': int,
        'comments_count': int,
        'permalink': str
    }

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_posts.csv', format=UTF8)

    def run(self):
        access_token = os.getenv('FB_ACCESS_TOKEN')
        if not access_token:
            raise EnvironmentError("FB Access token is not set")

        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['instagram']['pageId']

        all_media = []

        fields = ','.join(self.columns.keys())
        # use limit=100 to keep amount of requests small
        # 100 is the maximum value the Graph API will accept
        limit = 100

        media_url = (f'{API_BASE}/{page_id}/media'
                     f'?fields={fields}&limit={limit}')

        response = try_request_multiple_times(media_url)
        response_json = response.json()

        current_count = len(response_json['data'])
        all_media.extend(response_json['data'])

        logger.info("Fetching Instagram posts ...")
        while 'next' in response_json['paging']:
            next_url = response_json['paging']['next']
            response = try_request_multiple_times(next_url)
            response_json = response.json()

            current_count += len(response_json['data'])
            if sys.stdout.isatty():
                print(
                    f"\rFetched {current_count} Instagram posts",
                    end='',
                    flush=True)
            for media in response_json['data']:
                all_media.append(media)

            if self.minimal_mode:
                logger.info("Running in minimal mode, stopping now")
                response_json['paging'].pop('next')

        if sys.stdout.isatty():
            print()  # have to manually print newline

        logger.info("Fetching of Instagram posts complete")

        df = pd.DataFrame([
            {
                column: adapter(media[column])
                for (column, adapter)
                in self.columns.items()
            }
            for media
            in all_media
        ])
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class FetchIgPostPerformance(DataPreparationTask):
    """Fetch performance values for all fetched Instagram posts."""

    columns = luigi.parameter.ListParameter(description="Column names")
    timespan = luigi.parameter.TimeDeltaParameter(
        default=dt.timedelta(days=60),
        description="For how much time posts should be fetched")

    def _requires(self):
        # _requires introduces dependencies without
        # affecting the input() of a task
        return luigi.task.flatten([
            IgPostsToDb(),
            super()._requires()
        ])

    def requires(self):
        return FetchIgPosts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_post_performance.csv',
            format=UTF8)

    def run(self):
        with self.input().open('r') as input_file:
            post_df = pd.read_csv(input_file)

        if self.minimal_mode:
            post_df = post_df.head(5)

        generic_metrics = [
            'impressions',
            'reach',
            'engagement',
            'saved'
        ]

        performance_df = pd.DataFrame(columns=[
            column for column in self.columns
            if not column.startswith('delta_')])

        fetch_time = dt.datetime.now()
        for i, row in self.tqdm(
                post_df.iterrows(),
                desc="Fetching insights for instagram posts",
                total=len(post_df)):
            # Fetch only insights for less than 2 months old posts
            post_time = dtparser.parse(row['timestamp'])
            if post_time.date() < \
               fetch_time.date() - self.timespan:
                continue

            metrics = ','.join(generic_metrics)
            if row['media_type'] == 'VIDEO':
                metrics += ',video_views'  # causes error if used on non-video

            url = f'{API_BASE}/{row["id"]}/insights?metric={metrics}'

            response = try_request_multiple_times(url)
            response_data = response.json()['data']

            impressions = response_data[0]['values'][0]['value']
            reach = response_data[1]['values'][0]['value']
            engagement = response_data[2]['values'][0]['value']
            saved = response_data[3]['values'][0]['value']

            video_views = response_data[4]['values'][0]['value']\
                if row['media_type'] == 'VIDEO'\
                else 0  # for non-video posts

            performance_df.loc[i] = [
                str(row['id']),  # The type was lost during CSV conversion
                fetch_time,
                impressions,
                reach,
                engagement,
                saved,
                video_views
            ]

        performance_df = self.filter_fkey_violations(performance_df)
        performance_df = self.condense_performance_values(
            performance_df,
            delta_function=PerformanceValueCondenser.linear_delta
        )

        with self.output().open('w') as output_file:
            performance_df.to_csv(output_file, index=False, header=True)


class FetchIgProfileMetricsDevelopment(DataPreparationTask):
    """Fetch development values for the Instagram profile metrics."""

    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_profile_metrics_development.csv',
            format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['instagram']['pageId']

        df = pd.DataFrame(columns=self.columns)
        metrics = ','.join([
            'impressions',
            'reach',
            'profile_views',
            'follower_count',
            'website_clicks'
        ])
        period = 'day'
        url = f'{API_BASE}/{page_id}/insights?metric={metrics}&period={period}'
        response = try_request_multiple_times(url)
        response_data = response.json()['data']

        timestamp = response_data[0]['values'][0]['end_time']
        metrics = self.extract_metrics(response_data)

        df = df.append({'timestamp': timestamp, **metrics}, ignore_index=True)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)

    def extract_metrics(self, datas):
        return {
            data['name']: data['values'][0]['value']
            for data in datas
        }


class FetchIgTotalProfileMetrics(DataPreparationTask):
    """Fetch total metrics of an Instagram profile."""

    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_total_profile_metrics.csv',
            format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['instagram']['pageId']

        df = pd.DataFrame(columns=self.columns)
        fields = ','.join([
            'followers_count',
            'media_count'
        ])
        url = f'{API_BASE}/{page_id}?fields={fields}'
        response = try_request_multiple_times(url)
        response_data = response.json()

        timestamp = dt.datetime.now()

        follower_count = response_data.get('followers_count')
        media_count = response_data.get('media_count')

        df.loc[0] = [
            timestamp,
            follower_count,
            media_count
        ]

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class FetchIgAudienceOrigin(DataPreparationTask):
    """Fetch origin information about the Instagram audience."""

    columns = luigi.parameter.ListParameter(description="Column names")
    country_mode = luigi.parameter.BoolParameter(
        description="Whether to use countries instead of cities",
        default=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metric = 'country' if self.country_mode else 'city'

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_audience_{self.metric}.csv',
            format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['instagram']['pageId']

        df = pd.DataFrame(columns=self.columns)
        values = _get_single_metric(page_id, f'audience_{self.metric}')

        timstamp = dt.datetime.now()
        for i, (value, amount) in enumerate(values.items()):
            df.loc[i] = [
                value,
                timstamp,
                amount
            ]

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class FetchIgAudienceGenderAge(DataPreparationTask):
    """Fetch gender/age information about the Instagram audience."""

    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return MuseumFacts()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/instagram/ig_audience_gender_age.csv',
            format=UTF8)

    def run(self):
        with self.input().open('r') as facts_file:
            facts = json.load(facts_file)
        page_id = facts['ids']['instagram']['pageId']

        df = pd.DataFrame(columns=self.columns)
        values = _get_single_metric(page_id, 'audience_gender_age')

        timstamp = dt.datetime.now()
        for i, (gender_age, amount) in enumerate(values.items()):
            gender, age = gender_age.split('.')
            df.loc[i] = [
                gender,
                age,
                timstamp,
                amount
            ]

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


def _get_single_metric(page_id, metric, period='lifetime'):

    url = f'{API_BASE}/{page_id}/insights?metric={metric}&period={period}'
    res = try_request_multiple_times(url)
    return res.json()['data'][0]['values'][0]['value']

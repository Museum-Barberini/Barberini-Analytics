import datetime as dt
import json
import logging
import os
import sys

from dateutil import parser as dtparser
import luigi
import pandas as pd
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask
from facebook import API_BASE, try_request_multiple_times
from museum_facts import MuseumFacts

logger = logging.getLogger('luigi-interface')

# =============== Database Tasks ===============


class IgToDb(luigi.WrapperTask):
    def requires(self):
        yield IgPostsToDb()
        yield IgProfileMetricsDevelopmentToDb()
        yield IgTotalProfileMetricsToDb()
        yield IgAudienceGenderAgeToDb()
        yield IgAudienceCityToDb()
        yield IgAudienceCountryToDb()


class IgPostPerformanceToDb(CsvToDb):
    table = 'ig_post_performance'

    def requires(self):
        return FetchIgPostPerformance(
            table=self.table, columns=[col[0] for col in self.columns])


class IgPostsToDb(CsvToDb):
    table = 'ig_post'

    def requires(self):
        return FetchIgPosts()


class IgProfileMetricsDevelopmentToDb(CsvToDb):
    table = 'ig_profile_metrics_development'

    def requires(self):
        return FetchIgProfileMetricsDevelopment(
            table=self.table, columns=[col[0] for col in self.columns])


class IgTotalProfileMetricsToDb(CsvToDb):
    table = 'ig_total_profile_metrics'

    def requires(self):
        return FetchIgTotalProfileMetrics(
            table=self.table, columns=[col[0] for col in self.columns])


class IgAudienceCityToDb(CsvToDb):
    table = 'ig_audience_city'

    def requires(self):
        return FetchIgAudienceOrigin(
            table=self.table,
            columns=[col[0] for col in self.columns],
            country_mode=False)


class IgAudienceCountryToDb(CsvToDb):
    table = 'ig_audience_country'

    def requires(self):
        return FetchIgAudienceOrigin(
            table=self.table,
            columns=[col[0] for col in self.columns],
            country_mode=True)


class IgAudienceGenderAgeToDb(CsvToDb):
    table = 'ig_audience_gender_age'

    def requires(self):
        return FetchIgAudienceGenderAge(
            table=self.table, columns=[col[0] for col in self.columns])

# =============== DataPreparation Tasks ===============


class FetchIgPosts(DataPreparationTask):

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

        performance_df = pd.DataFrame(columns=self.columns)

        fetch_time = dt.datetime.now()
        for i, row in post_df.iterrows():
            # Fetch only insights for less than 2 months old posts
            post_time = dtparser.parse(row['timestamp'])
            if post_time.date() < \
               fetch_time.date() - self.timespan:
                continue

            metrics = ','.join(generic_metrics)
            if sys.stdout.isatty():
                print(
                    f"\rFetched insight for instagram post from {post_time}",
                    end='',
                    flush=True)

            if row['media_type'] == 'VIDEO':
                metrics += ',video_views'  # causes error if used on non-video

            url = f'{API_BASE}/{row["id"]}/insights?metric={metrics}'

            response = try_request_multiple_times(url)
            response_data = response.json()['data']

            impressions = response_data[0]['values'][0]['value']
            reach = response_data[1]['values'][0]['value']
            engagement = response_data[2]['values'][0]['value']
            saved = response_data[3]['values'][0]['value']

            if row['media_type'] == 'VIDEO':
                video_views = response_data[4]['values'][0]['value']
            else:
                video_views = 0  # for non-video posts

            performance_df.loc[i] = [
                str(row['id']),  # The type was lost during CSV conversion
                fetch_time,
                impressions,
                reach,
                engagement,
                saved,
                video_views
            ]

        if sys.stdout.isatty():
            print()

        performance_df = self.filter_fkey_violations(performance_df)
        performance_df = self.condense_performance_values(performance_df)

        with self.output().open('w') as output_file:
            performance_df.to_csv(output_file, index=False, header=True)


class FetchIgProfileMetricsDevelopment(DataPreparationTask):
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

        impressions = response_data[0]['values'][0]['value']
        reach = response_data[1]['values'][0]['value']
        profile_views = response_data[2]['values'][0]['value']
        follower_count = response_data[3]['values'][0]['value']
        website_clicks = response_data[4]['values'][0]['value']

        df.loc[0] = [
            timestamp,
            impressions,
            reach,
            profile_views,
            follower_count,
            website_clicks
        ]

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)


class FetchIgTotalProfileMetrics(DataPreparationTask):
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
        values = get_single_metric(page_id, f'audience_{self.metric}')

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
        values = get_single_metric(page_id, 'audience_gender_age')

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

# =============== Utilities ===============


def get_single_metric(page_id, metric, period='lifetime'):
    url = f'{API_BASE}/{page_id}/insights?metric={metric}&period={period}'
    res = try_request_multiple_times(url)
    return res.json()['data'][0]['values'][0]['value']

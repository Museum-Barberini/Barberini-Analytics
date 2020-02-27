import datetime as dt
import json
import os

import luigi
import pandas as pd
import psycopg2
import requests
from luigi.format import UTF8

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options


class FetchFbPosts(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def output(self):
        return luigi.LocalTarget("output/facebook/fb_posts.csv", format=UTF8)

    def run(self):

        access_token = os.environ['FB_ACCESS_TOKEN']
        with open('data/barberini-facts.json') as facts_json:
            barberini_facts = json.load(facts_json)
            page_id = barberini_facts['ids']['facebook']['pageId']

        posts = []

        url = f"https://graph.facebook.com/{page_id}/posts?access_token={access_token}"

        response = requests.get(url)
        response.raise_for_status()

        response_content = response.json()
        for post in (response_content['data']):
            posts.append(post)

        print("Fetching facebook posts ...")
        page_count = 0
        while ('next' in response_content['paging']):
            page_count = page_count + 1
            url = response_content['paging']['next']
            response = requests.get(url)
            response.raise_for_status()

            response_content = response.json()
            for post in (response_content['data']):
                posts.append(post)
            print(f"\rFetched facebook page {page_count}", end='', flush=True)
        print("Fetching of facebook posts completed")

        with self.output().open('w') as output_file:
            df = pd.DataFrame([post for post in posts])
            df = df.filter(['created_time', 'message', 'id'])
            df.columns = ['post_date', 'text', 'fb_post_id']
            df.to_csv(output_file, index=False, header=True)


class FetchFbPostPerformance(luigi.Task):

    def requires(self):
        return FetchFbPosts()

    def output(self):
        return luigi.LocalTarget(
            "output/facebook/fb_post_performances.csv", format=UTF8)

    def run(self):
        access_token = os.environ['FB_ACCESS_TOKEN']

        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        performances = []
        df = pd.read_csv(self.input().path)

        for index in df.index:
            post_id = df['fb_post_id'][index]
            # print(f"### Facebook - loading performance data for post
            # {str(post_id)} ###")
            url = f"https://graph.facebook.com/{post_id}/insights?access_token={access_token}&metric=post_reactions_by_type_total,post_activity_by_action_type,post_clicks_by_type,post_negative_feedback"
            response = requests.get(url)
            response.raise_for_status()

            response_content = response.json()

            post_perf = dict()
            post_perf["fb_post_id"] = post_id
            post_perf["time_stamp"] = current_timestamp

            # Reactions
            reactions = response_content['data'][0]['values'][0]['value']
            post_perf["react_like"] = int(reactions.get('like', 0))
            post_perf["react_love"] = int(reactions.get('love', 0))
            post_perf["react_wow"] = int(reactions.get('wow', 0))
            post_perf["react_haha"] = int(reactions.get('haha', 0))
            post_perf["react_sorry"] = int(reactions.get('sorry', 0))
            post_perf["react_anger"] = int(reactions.get('anger', 0))

            # Activity
            activity = response_content['data'][1]['values'][0]['value']
            post_perf["likes"] = int(activity.get('like', 0))
            post_perf["shares"] = int(activity.get('share', 0))
            post_perf["comments"] = int(activity.get('comment', 0))

            # Clicks
            clicks = response_content['data'][2]['values'][0]['value']
            post_perf["video_clicks"] = int(clicks.get('video play', 0))
            post_perf["link_clicks"] = int(clicks.get('link clicks', 0))
            post_perf["other_clicks"] = int(clicks.get('other clicks', 0))

            # negative feedback (only one field)
            post_perf["negative_feedback"] = clicks = response_content['data'][3]['values'][0]['value']

            performances.append(post_perf)

        with self.output().open('w') as output_file:
            df = pd.DataFrame([perf for perf in performances])
            df.to_csv(output_file, index=False, header=True)


class FbPostsToDB(CsvToDb):

    table = "fb_post"

    columns = [
        ("post_date", "TIMESTAMP"),
        ("text", "TEXT"),
        ("fb_post_id", "TEXT")
    ]

    primary_key = 'fb_post_id'

    def requires(self):
        return FetchFbPosts()


class FbPostPerformanceToDB(CsvToDb):

    table = "fb_post_performance"

    columns = [
        ("fb_post_id", "TEXT"),
        ("time_stamp", "TIMESTAMP"),
        ("react_like", "INT"),
        ("react_love", "INT"),
        ("react_wow", "INT"),
        ("react_haha", "INT"),
        ("react_sorry", "INT"),
        ("react_anger", "INT"),
        ("likes", "INT"),
        ("shares", "INT"),
        ("comments", "INT"),
        ("video_clicks", "INT"),
        ("link_clicks", "INT"),
        ("other_clicks", "INT"),
        ("negative_feedback", "INT")
    ]

    primary_key = ('fb_post_id', 'time_stamp')

    foreign_keys = [
        {
            "origin_column": "fb_post_id",
            "target_table": "fb_post",
            "target_column": "fb_post_id"
        }
    ]

    def requires(self):
        return FetchFbPostPerformance()

import luigi
import psycopg2
from luigi.format import UTF8
import datetime as dt
import pandas as pd
import twitterscraper as ts
import json

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options


class FetchTwitter(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
    
    query = luigi.Parameter(default = "museumbarberini")
    table = luigi.Parameter(default="tweets")

    def output(self):
        return luigi.LocalTarget("output/raw_tweets.csv", format=UTF8)

    def run(self):
        
        tweets = ts.query_tweets(self.query, begindate=dt.date(2015, 1, 1))
        
        with self.output().open('w') as output_file:
            df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
            df.to_csv(output_file, index=False, header=True)


class ExtractBarberiniTweets(luigi.Task):
    def requires(self):
        return FetchTwitter()
    

    def barberini_user_id(self):
        with open('data/barberini-facts.json') as facts_json:
            barberini_facts = json.load(facts_json)
            return barberini_facts['ids']['twitter']['user_id']

    def run(self):
        df = pd.read_csv(self.input().path)
        filtered_df = df[df['user_id'] == self.barberini_user_id()]
        projected_df = filtered_df.filter(['tweet_id', 'text', 'parent_tweet_id', 'timestamp'])
        projected_df.columns = ['tweet_id', 'text', 'response_to', 'post_date']
        with self.output().open('w') as output_file:
            projected_df.to_csv(output_file, index=False, header=True)


    def output(self):
        return luigi.LocalTarget("output/barberini_tweets.csv", format=UTF8)


#TODO: GET RID OF THIS TERRIBLE CODE DUPLICATION
class ExtractUserTweets(luigi.Task):
    def requires(self):
        return FetchTwitter()
    

    def barberini_user_id(self):
        with open('data/barberini-facts.json') as facts_json:
            barberini_facts = json.load(facts_json)
            return barberini_facts['ids']['twitter']['user_id']

    def run(self):
        df = pd.read_csv(self.input().path)
        filtered_df = df[df['user_id'] != self.barberini_user_id()]
        projected_df = filtered_df.filter(['tweet_id', 'user_id', 'text', 'parent_tweet_id', 'timestamp'])
        projected_df.columns = ['tweet_id', 'user_id', 'text', 'response_to', 'post_date']
        with self.output().open('w') as output_file:
            projected_df.to_csv(output_file, index=False, header=True)

    
    def output(self):
        return luigi.LocalTarget("output/user_tweets.csv", format=UTF8)


class ExtractPerformanceTweets(luigi.Task):
    def requires(self):
        return FetchTwitter()

    def run(self):
        df = pd.read_csv(self.input().path)
        projected_df = df.filter(['tweet_id', 'likes', 'retweets', 'replies'])
        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        projected_df['timestamp'] = current_timestamp
        with self.output().open('w') as output_file:
            projected_df.to_csv(output_file, index=False, header=True)

    
    def output(self):
        return luigi.LocalTarget("output/performance_tweets.csv", format=UTF8)


class BarberiniTweetsToDB(CsvToDb):

    table = "barberini_tweet"

    columns = [
        ("tweet_id", "TEXT"),
        ("text", "TEXT"),
        ("response_to", "TEXT"),
        ("post_date", "DATE")
    ]

    def requires(self):
        return ExtractBarberiniTweets()


class UserTweetsToDB(CsvToDb):

    table = "user_tweet"

    columns = [
        ("tweet_id", "TEXT"),
        ("user_id", "TEXT"),
        ("text", "TEXT"),
        ("response_to", "TEXT"),
        ("post_date", "DATE")
    ]

    def requires(self):
        return ExtractUserTweets()


class PerformanceTweetsToDB(CsvToDb):

    table = "performance_tweet"

    columns = [
        ("tweet_id", "TEXT"),
        ("likes", "INT"),
        ("retweets", "INT"),
        ("replies", "INT"),
        ("timestamp", "DATE")
    ]

    def requires(self):
        return ExtractPerformanceTweets()
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
    min_timestamp = luigi.DateParameter(default=dt.date(2015, 1, 1))
    
    def output(self):
        return luigi.LocalTarget("output/twitter/raw_tweets.csv", format=UTF8)
    
    def run(self):
        
        tweets = ts.query_tweets(self.query, begindate=self.min_timestamp)
        df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
        df = df.drop_duplicates(subset=["tweet_id"])
        
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)
    
    #def get_latest_timestamp(self):
    #	
    #	try:
    #		conn = psycopg2.connect(
    #			host=self.host, database=self.database,
    #			user=self.user, password=self.password
    #		)
    #		cur = conn.cursor()
    #		cur.execute(f"SELECT MAX(timestamp) FROM {self.table}")
    #		return cur.fetchone()[0] or self.min_timestamp
    #		conn.close()
    #	
    #	except psycopg2.DatabaseError as error:
    #		print(error)
    #		if conn is not None:
    #			conn.close()
    #		return self.min_timestamp

class ExtractTweets(luigi.Task):
    def requires(self):
        return FetchTwitter()
    
    def barberini_user_id(self):
        with open('data/barberini-facts.json') as facts_json:
            barberini_facts = json.load(facts_json)
            return barberini_facts['ids']['twitter']['user_id']
    
    def run(self):
        df = pd.read_csv(self.input().path)
        df = df.filter(['user_id', 'tweet_id', 'text', 'parent_tweet_id', 'timestamp'])
        df.columns = ['user_id', 'tweet_id', 'text', 'response_to', 'post_date']
        df['is_from_barberini'] = df['user_id'] == self.barberini_user_id()
        df = df.drop_duplicates()
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)
    
    def output(self):
        return luigi.LocalTarget("output/twitter/tweets.csv", format=UTF8)


class ExtractPerformanceTweets(luigi.Task):
    def requires(self):
        return FetchTwitter()
    
    def run(self):
        df = pd.read_csv(self.input().path)
        df = df.filter(['tweet_id', 'likes', 'retweets', 'replies'])
        current_timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['timestamp'] = current_timestamp
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, header=True)
    
    def output(self):
        return luigi.LocalTarget("output/twitter/performance_tweets.csv", format=UTF8)



class TweetsToDB(CsvToDb):
    
    table = "tweet"
    
    columns = [
        ("user_id", "TEXT"),
        ("tweet_id", "TEXT"),
        ("text", "TEXT"),
        ("response_to", "TEXT"),
        ("post_date", "DATE"),
        ("is_from_barberini", "BOOL")
    ]
    
    primary_key = 'tweet_id'
    
    def requires(self):
        return ExtractTweets()


class TweetPerformanceToDB(CsvToDb):
    
    table = "tweet_performance"
    
    columns = [
        ("tweet_id", "TEXT"),
        ("likes", "INT"),
        ("retweets", "INT"),
        ("replies", "INT"),
        ("timestamp", "DATE")
    ]
    
    primary_key = ('tweet_id', 'timestamp')
    
    def requires(self):
        return ExtractPerformanceTweets()

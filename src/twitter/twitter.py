import luigi
import psycopg2
from luigi.format import UTF8
import datetime as dt
import pandas as pd
import twitterscraper as ts

from csv_to_db import CsvToDb
from set_db_connection_options import set_db_connection_options


class FetchTwitter(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
    
    query = luigi.Parameter(default = "museumbarberini")
    table = luigi.Parameter(default="tweets")

    def output(self):
        return luigi.LocalTarget("output/tweets.csv", format=UTF8)

    def run(self):
        
        tweets = ts.query_tweets(self.query, begindate=self.get_latest_timestamp())
        tweets_df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
        tweets_df = tweets_df.drop_duplicates(subset=["tweet_id"])
        
        with self.output().open('w') as output_file:
            tweets_df.to_csv(output_file, index=False, header=True)
    
    def get_latest_timestamp(self):

        min_timestamp = dt.date(2015, 1, 1)

        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(timestamp) FROM {self.table}")
            return cur.fetchone()[0] or min_timestamp
            conn.close()
        
        except psycopg2.DatabaseError as error:
            print(error)
            if conn is not None:
                conn.close()
            return self.min_timestmap

class TweetsToDB(CsvToDb):

    table = "tweets"

    columns = [
        ("screen_name", "TEXT"),
        ("username", "TEXT"),
        ("user_id", "TEXT"),
        ("tweet_id", "TEXT"),
        ("tweet_url", "TEXT"),
        ("timestamp", "DATE"),
        ("timestamp_epochs", "INT"),
        ("text", "TEXT"),
        ("text_html", "TEXT"),
        ("links", "TEXT"),
        ("hashtags", "TEXT"),
        ("has_media", "TEXT"),
        ("img_urls", "TEXT"),
        ("video_url", "TEXT"),
        ("likes", "INT"),
        ("retweets", "INT"),
        ("replies", "INT"),
        ("is_replied", "TEXT"),
        ("is_reply_to", "TEXT"),
        ("parent_tweet_id", "TEXT"),
        ("reply_to_users", "TEXT")
    ]
    
    primary_key = ("tweet_id")

    def requires(self):
        return FetchTwitter()
 

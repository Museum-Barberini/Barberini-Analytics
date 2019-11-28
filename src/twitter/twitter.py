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
        
        with self.output().open('w') as output_file:
            df = pd.DataFrame([tweet.__dict__ for tweet in tweets])
            df.to_csv(output_file, index=False, header=True)

    def get_latest_timestamp(self):

        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()
            cur.execute("SELECT MAX(timestamp) FROM {0}".format(self.table))
            return cur.fetchone()[0]
            conn.close()
        
        # In case an error occurs (e.g. the DB is empty)
        except psycopg2.DatabaseError as error:
            print(error)
            return dt.date(2015, 1, 1)
            if conn is not None:
                conn.close()

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

    def requires(self):
        return FetchTwitter()
 

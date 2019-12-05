import luigi
from twitter.twitter import TweetsToDB
from google_trends.gtrends_interest_table import GtrendsInterestToDB
from google_trends.gtrends_topics_table import GtrendsTopicsToDB
from apple_appstore.fetch_apple_app_reviews import AppstoreReviewsToDB
from customers_to_db import CustomersToDB

class FillDB(luigi.WrapperTask):

	def requires(self):
		yield TweetsToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()
		yield CustomersToDB()


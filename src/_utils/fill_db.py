import luigi
from twitter.twitter import TweetsToDB, TweetPerformanceToDB
from google_trends.gtrends_interest_table import GtrendsInterestToDB
from google_trends.gtrends_topics_table import GtrendsTopicsToDB
from apple_appstore.fetch_apple_app_reviews import AppstoreReviewsToDB
from gomus.customers_to_db import CustomersToDB
from gomus.bookings_to_db import BookingsToDB

class FillDB(luigi.WrapperTask):

	def requires(self):
		yield TweetsToDB()
		yield TweetPerformanceToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()
		yield CustomersToDB()
		yield BookingsToDB()



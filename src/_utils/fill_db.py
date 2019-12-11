import luigi
from twitter.twitter import TweetsToDB
from google_trends.gtrends_interest_table import GtrendsInterestToDB
from google_trends.gtrends_topics_table import GtrendsTopicsToDB
from apple_appstore.fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook.facebook import FbPostsToDB, FbPostPerformanceToDB
from gomus.customers_to_db import CustomersToDB
from gomus.bookings_to_db import BookingsToDB

class FillDB(luigi.WrapperTask):

	def requires(self):
		yield TweetsToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()
		yield FbPostsToDB()
		yield FbPostPerformanceToDB()
		yield CustomersToDB()
		yield BookingsToDB()



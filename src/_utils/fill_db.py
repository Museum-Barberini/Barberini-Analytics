import luigi
from twitter import TweetsToDB, TweetPerformanceToDB
from gtrends_interest_table import GtrendsInterestToDB
from gtrends_topics_table import GtrendsTopicsToDB
from fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from customers_to_db import CustomersToDB
from bookings_to_db import BookingsToDB


class FillDB(luigi.WrapperTask):
	def requires(self):
		yield FillDBDaily()
		yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):
	def requires(self):
		yield TweetsToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()
		yield FbPostsToDB()
		yield CustomersToDB()
		yield BookingsToDB()


class FillDBHourly(luigi.WrapperTask):
	def requires(self):
		yield TweetPerformanceToDB()
		yield FbPostPerformanceToDB()

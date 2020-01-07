import luigi
from twitter import TweetsToDB, TweetPerformanceToDB
from gtrends_interest_table import GtrendsInterestToDB
from gtrends_topics_table import GtrendsTopicsToDB
from fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from customers_to_db import CustomersToDB
from bookings_to_db import BookingsToDB
from order_contains_to_db import OrderContainsToDB
from orders_to_db import OrdersToDB

class FillDB(luigi.WrapperTask):

	def requires(self):
		yield TweetsToDB()
		yield TweetPerformanceToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()
		yield FbPostsToDB()
		yield FbPostPerformanceToDB()
		yield CustomersToDB()
		yield BookingsToDB()
		yield OrderContainsToDB()
		yield OrdersToDB()


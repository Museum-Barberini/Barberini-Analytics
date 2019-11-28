import luigi
from twitter.twitter import BarberiniTweetsToDB, UserTweetsToDB, PerformanceTweetsToDB
from google_trends.gtrends_interest_table import GtrendsInterestToDB
from google_trends.gtrends_topics_table import GtrendsTopicsToDB
from apple_appstore.fetch_apple_app_reviews import AppstoreReviewsToDB

class FillDB(luigi.WrapperTask):

	def requires(self):
		yield BarberiniTweetsToDB()
		yield UserTweetsToDB()
		yield PerformanceTweetsToDB()
		yield GtrendsInterestToDB()
		yield GtrendsTopicsToDB()
		yield AppstoreReviewsToDB()


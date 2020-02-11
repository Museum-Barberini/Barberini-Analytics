import luigi
from twitter import TweetsToDB, TweetPerformanceToDB
from gtrends_values import GtrendsValuesToDB
from gtrends_topics_table import GtrendsTopicsToDB
from fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from customers_to_db import CustomersToDB
from bookings_to_db import BookingsToDB
from public_tours_to_db import PublicToursToDB
from order_contains_to_db import OrderContainsToDB
from orders_to_db import OrdersToDB
from fetch_google_maps_reviews import GoogleMapsReviewsToDB


class FillDB(luigi.WrapperTask):
    def requires(self):
        yield FillDBDaily()
        yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):
    def requires(self):
        yield TweetsToDB()
        yield GtrendsValuesToDB()
        yield GtrendsTopicsToDB()
        yield AppstoreReviewsToDB()
        yield FbPostsToDB()
        yield CustomersToDB()
        yield BookingsToDB()
        yield PublicToursToDB()
        yield OrderContainsToDB()
        yield OrdersToDB()
        yield GoogleMapsReviewsToDB()


class FillDBHourly(luigi.WrapperTask):
    def requires(self):
        yield TweetPerformanceToDB()
        yield FbPostPerformanceToDB()


import luigi
from twitter import TweetsToDB, TweetPerformanceToDB
from gtrends_interest_table import GtrendsInterestToDB
from gtrends_topics_table import GtrendsTopicsToDB
from fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from customers_to_db import CustomersToDB
from bookings_to_db import BookingsToDB
from events_to_db import EventsToDB
from order_contains_to_db import OrderContainsToDB
from orders_to_db import OrdersToDB
from fetch_google_maps_reviews import GoogleMapsReviewsToDB
from daily_entries_to_db import DailyEntriesToDB, ExpectedDailyEntriesToDB


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
        yield EventsToDB()
        yield OrderContainsToDB()
        yield OrdersToDB()
        yield GoogleMapsReviewsToDB()
        yield DailyEntriesToDB()
        yield ExpectedDailyEntriesToDB()


class FillDBHourly(luigi.WrapperTask):
    def requires(self):
        yield TweetPerformanceToDB()
        yield FbPostPerformanceToDB()

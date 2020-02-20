import luigi

from apple_appstore.fetch_apple_app_reviews import AppstoreReviewsToDB
from facebook.facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps.fetch_google_maps_reviews import GoogleMapsReviewsToDB
from twitter.twitter import TweetsToDB, TweetPerformanceToDB

from google_trends.gtrends_interest_table import GtrendsInterestToDB
from google_trends.gtrends_topics_table import GtrendsTopicsToDB

from gomus.bookings import BookingsToDB
from gomus.customers import CustomersToDB
from gomus.daily_entries import DailyEntriesToDB, ExpectedDailyEntriesToDB
from gomus.events import EventsToDB
from gomus.order_contains import OrderContainsToDB
from gomus.orders import OrdersToDB


class FillDB(luigi.WrapperTask):
    def requires(self):
        yield FillDBDaily()
        yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):
    def requires(self):
        yield AppstoreReviewsToDB()
        yield FbPostsToDB()
        yield GoogleMapsReviewsToDB()
        yield TweetsToDB()
        yield GtrendsValuesToDB()

        yield BookingsToDB()
        yield CustomersToDB()
        yield DailyEntriesToDB()
        yield ExpectedDailyEntriesToDB()
        yield EventsToDB()
        yield OrderContainsToDB()
        yield OrdersToDB()


class FillDBHourly(luigi.WrapperTask):
    def requires(self):
        yield FbPostPerformanceToDB()
        yield TweetPerformanceToDB()

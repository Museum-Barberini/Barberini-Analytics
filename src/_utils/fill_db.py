import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from twitter import TweetsToDB, TweetPerformanceToDB
from google_trends.gtrends_values import GtrendsValuesToDB
from gomus.bookings import BookingsToDB
from gomus.customers import CustomersToDB, GomusToCustomerMappingToDB
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

        yield GomusToCustomerMappingToDB()


class FillDBHourly(luigi.WrapperTask):
    def requires(self):
        yield FbPostPerformanceToDB()
        yield TweetPerformanceToDB()

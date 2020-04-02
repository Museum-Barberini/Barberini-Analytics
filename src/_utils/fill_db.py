import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from twitter import TweetsToDB, TweetPerformanceToDB
from google_trends.gtrends_values import GtrendsValuesToDB
from gomus.bookings import BookingsToDB
from gomus.customers import CustomersToDB, GomusToCustomerMappingToDB
from gomus.daily_entries import DailyEntriesToDB, ExpectedDailyEntriesToDB
from gomus.events import EventsToDB
# from gomus.order_contains import OrderContainsToDB
from gomus.orders import OrdersToDB


class FillDB(luigi.WrapperTask):

    def requires(self):
        yield FillDBDaily()
        yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):

    def requires(self):
        yield AppstoreReviewsToDB()
        yield GooglePlaystoreReviewsToDB()
        yield FbPostsToDB()
        yield GoogleMapsReviewsToDB()
        yield TweetsToDB()
        yield GtrendsValuesToDB()

        yield BookingsToDB(minimal=self.minimal)
        yield CustomersToDB(minimal=self.minimal)
        yield DailyEntriesToDB()
        yield ExpectedDailyEntriesToDB()
        yield EventsToDB(minimal=self.minimal)
        yield GomusToCustomerMappingToDB(minimal=self.minimal)
        # yield OrderContainsToDB(minimal=self.minimal)
        yield OrdersToDB(minimal=self.minimal)


class FillDBHourly(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDB()
        yield TweetPerformanceToDB()

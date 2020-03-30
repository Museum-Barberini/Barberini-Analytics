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
from gomus.order_contains import OrderContainsToDB
from gomus.orders import OrdersToDB


class FillDB(luigi.WrapperTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    def requires(self):
        yield FillDBDaily(minimal=self.minimal)
        yield FillDBHourly(minimal=self.minimal)


class FillDBDaily(luigi.WrapperTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    def requires(self):
        yield AppstoreReviewsToDB(minimal=self.minimal)
        yield GooglePlaystoreReviewsToDB()
        yield FbPostsToDB(minimal=self.minimal)
        yield GoogleMapsReviewsToDB(minimal=self.minimal)
        yield TweetsToDB(minimal=self.minimal)
        yield GtrendsValuesToDB(minimal=self.minimal)

        yield BookingsToDB(minimal=self.minimal)
        yield CustomersToDB(minimal=self.minimal)
        yield DailyEntriesToDB(minimal=self.minimal)
        yield ExpectedDailyEntriesToDB(minimal=self.minimal)
        yield EventsToDB(minimal=self.minimal)
        yield OrderContainsToDB(minimal=self.minimal)
        yield OrdersToDB(minimal=self.minimal)

        yield GomusToCustomerMappingToDB(minimal=self.minimal)


class FillDBHourly(luigi.WrapperTask):
    minimal = luigi.parameter.BoolParameter(default=False)

    def requires(self):
        yield FbPostPerformanceToDB(minimal=self.minimal)
        yield TweetPerformanceToDB(minimal=self.minimal)
        pass

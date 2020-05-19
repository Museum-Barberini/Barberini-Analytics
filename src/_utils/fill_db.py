import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from google_trends.gtrends_values import GtrendsValuesToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from instagram import IgToDBWrapper, IgPostPerformanceToDB
# from twitter import TweetsToDB, TweetPerformanceToDB, TweetAuthorsToDB

from gomus.bookings import BookingsToDB
from gomus.customers import CustomersToDB, GomusToCustomerMappingToDB
from gomus.daily_entries import DailyEntriesToDB, ExpectedDailyEntriesToDB
from gomus.events import EventsToDB
from gomus.order_contains import OrderContainsToDB
from gomus.orders import OrdersToDB

# TODO remove tomorrow
import datetime as dt


class FillDB(luigi.WrapperTask):

    def requires(self):
        yield FillDBDaily()
        yield FillDBHourly()


class FillDBDaily(luigi.WrapperTask):

    def requires(self):
        # === WWW channels ===
        yield AppstoreReviewsToDB()
        yield FbPostsToDB()
        yield GoogleMapsReviewsToDB()
        yield GtrendsValuesToDB()
        yield GooglePlaystoreReviewsToDB()
        yield IgToDBWrapper()
        # yield TweetAuthorsToDB()
        # yield TweetsToDB()

        # === Gomus ===

        # START TODO: remove this tomorrow
        yield BookingsToDB(timespan='_1month')
        yield CustomersToDB(today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield CustomersToDB(today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield CustomersToDB(today=dt.datetime.today()-dt.timedelta(weeks=3))
        yield GomusToCustomerMappingToDB(
            today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield GomusToCustomerMappingToDB(
            today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield GomusToCustomerMappingToDB(
            today=dt.datetime.today()-dt.timedelta(weeks=3))
        yield OrdersToDB(today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield OrdersToDB(today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield OrdersToDB(today=dt.datetime.today()-dt.timedelta(weeks=3))
        # IMPORTANT NOTE : var in gomus/events needs to be reset to 2 weeks
        # END

        yield BookingsToDB()
        yield CustomersToDB()
        yield DailyEntriesToDB()
        yield ExpectedDailyEntriesToDB()
        yield EventsToDB()
        yield GomusToCustomerMappingToDB()
        yield OrderContainsToDB()
        yield OrdersToDB()


class FillDBHourly(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDB()
        yield IgPostPerformanceToDB()
        # yield TweetPerformanceToDB()

import luigi

from apple_appstore import AppstoreReviewsToDb
from facebook import FbPostsToDb, FbPostCommentsToDb, FbPostPerformanceToDb
from google_maps import GoogleMapsReviewsToDb
from google_trends.gtrends_values import GtrendsValuesToDb
from gplay.gplay_reviews import GooglePlaystoreReviewsToDb
from instagram import IgToDbWrapper, IgPostPerformanceToDb
from twitter import TweetsToDb, TweetPerformanceToDb, TweetAuthorsToDb

from gomus.bookings import BookingsToDb
from gomus.customers import CustomersToDb, GomusToCustomerMappingToDb
from gomus.daily_entries import DailyEntriesToDb, ExpectedDailyEntriesToDb
from gomus.events import EventsToDb
from gomus.order_contains import OrderContainsToDb
from gomus.orders import OrdersToDb

# TODO remove tomorrow
import datetime as dt


class FillDb(luigi.WrapperTask):

    def requires(self):
        yield FillDbDaily()
        yield FillDbHourly()


class FillDbDaily(luigi.WrapperTask):

    def requires(self):
        # === WWW channels ===
        yield AppstoreReviewsToDb()
        yield FbPostsToDb()
        yield FbPostCommentsToDb()
        yield GoogleMapsReviewsToDb()
        yield GtrendsValuesToDb()
        yield GooglePlaystoreReviewsToDb()
        yield IgToDbWrapper()
        yield TweetAuthorsToDb()
        yield TweetsToDb()

        # === Gomus ===

        # START TODO: remove this tomorrow
        yield BookingsToDb(timespan='_1month')
        yield CustomersToDb(today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield CustomersToDb(today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield CustomersToDb(today=dt.datetime.today()-dt.timedelta(weeks=3))
        yield GomusToCustomerMappingToDb(
            today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield GomusToCustomerMappingToDb(
            today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield GomusToCustomerMappingToDb(
            today=dt.datetime.today()-dt.timedelta(weeks=3))
        yield OrdersToDb(today=dt.datetime.today()-dt.timedelta(weeks=1))
        yield OrdersToDb(today=dt.datetime.today()-dt.timedelta(weeks=2))
        yield OrdersToDb(today=dt.datetime.today()-dt.timedelta(weeks=3))
        # IMPORTANT NOTE : var in gomus/events needs to be reset to 2 weeks
        # END

        yield BookingsToDb()
        yield CustomersToDb()
        yield DailyEntriesToDb()
        yield ExpectedDailyEntriesToDb()
        yield EventsToDb()
        yield GomusToCustomerMappingToDb()
        yield OrderContainsToDb()
        yield OrdersToDb()


class FillDbHourly(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDb()
        yield IgPostPerformanceToDb()
        yield TweetPerformanceToDb()

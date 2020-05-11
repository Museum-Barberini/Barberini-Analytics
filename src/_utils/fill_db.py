import luigi

from apple_appstore import AppstoreReviewsToDB
from facebook import FbPostsToDB, FbPostPerformanceToDB
from google_maps import GoogleMapsReviewsToDB
from google_trends.gtrends_values import GtrendsValuesToDB
from gplay.gplay_reviews import GooglePlaystoreReviewsToDB
from instagram import IgToDBWrapper, IgPostPerformanceToDB
from twitter import TweetsToDB, TweetPerformanceToDB, TweetAuthorsToDB

# from gomus.bookings import BookingsToDB
# from gomus.customers import CustomersToDB, GomusToCustomerMappingToDB
from gomus.daily_entries import DailyEntriesToDB, ExpectedDailyEntriesToDB
# from gomus.events import EventsToDB
# from gomus.order_contains import OrderContainsToDB
# from gomus.orders import OrdersToDB
from gomus._utils.cleanse_data import CleansePostalCodes


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
        yield TweetAuthorsToDB()
        yield TweetsToDB()

        # === Gomus ===
        # yield BookingsToDB()
        # yield CustomersToDB()
        yield DailyEntriesToDB()
        yield ExpectedDailyEntriesToDB()
        # yield EventsToDB()
        # yield GomusToCustomerMappingToDB()
        # yield OrderContainsToDB()
        # yield OrdersToDB()

        # Data analysis - here until further analyses are added
        yield CleansePostalCodes()


class FillDBHourly(luigi.WrapperTask):

    def requires(self):
        yield FbPostPerformanceToDB()
        yield IgPostPerformanceToDB()
        yield TweetPerformanceToDB()

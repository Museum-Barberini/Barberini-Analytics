# TODO remove this 
import datetime as dt

import luigi

from gomus.bookings import BookingsToDB
from gomus.customers import CustomersToDB, GomusToCustomerMappingToDB
from gomus.daily_entries import DailyEntriesToDB, ExpectedDailyEntriesToDB
from gomus.exhibitions import ExhibitionTimesToDb
from gomus.events import EventsToDB
from gomus.order_contains import OrderContainsToDB
from gomus.orders import OrdersToDB


class GomusToDb(luigi.WrapperTask):

    light_mode = luigi.BoolParameter(
        description=("If enabled, expensive tasks won't be run"
                     " (activate this when the go~mus servers have stress)"),
        default=False
    )

    def requires(self):

        # START TODO: remove this tomorrow
        yield BookingsToDB(timespan='_1month')
        yield CustomersToDB(today=dt.datetime.today() - dt.timedelta(weeks=1))
        yield CustomersToDB(today=dt.datetime.today() - dt.timedelta(weeks=2))
        yield GomusToCustomerMappingToDB(
            today=dt.datetime.today() - dt.timedelta(weeks=1))
        yield GomusToCustomerMappingToDB(
            today=dt.datetime.today() - dt.timedelta(weeks=2))
        yield OrdersToDB(today=dt.datetime.today() - dt.timedelta(weeks=1))
        yield OrdersToDB(today=dt.datetime.today() - dt.timedelta(weeks=2))
        # END

        yield DailyEntriesToDB()
        yield ExhibitionTimesToDb()
        yield ExpectedDailyEntriesToDB()

        if not self.light_mode:
            yield BookingsToDB()
            yield CustomersToDB()
            yield EventsToDB()
            yield GomusToCustomerMappingToDB()
            yield OrderContainsToDB()
            yield OrdersToDB()

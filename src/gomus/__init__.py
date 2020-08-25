"""Provides tasks for downloading data from the museum system, go~mus."""

import luigi

from gomus.bookings import BookingsToDb
from gomus.customers import CustomersToDb, GomusToCustomerMappingToDb
from gomus.daily_entries import DailyEntriesToDb, ExpectedDailyEntriesToDb
from gomus.exhibitions import ExhibitionTimesToDb
from gomus.events import EventsToDb
from gomus.order_contains import OrderContainsToDb
from gomus.orders import OrdersToDb


class GomusToDb(luigi.WrapperTask):

    light_mode = luigi.BoolParameter(
        description=("If enabled, expensive tasks won't be run"
                     " (activate this when the go~mus servers have stress)"),
        default=False
    )

    def requires(self):

        yield DailyEntriesToDb()
        yield ExhibitionTimesToDb()
        yield ExpectedDailyEntriesToDb()

        if not self.light_mode:
            yield BookingsToDb()
            yield CustomersToDb()
            yield EventsToDb()
            yield GomusToCustomerMappingToDb()
            yield OrderContainsToDb()
            yield OrdersToDb()

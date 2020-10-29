"""Provides tasks for downloading data from the museum system, go~mus."""

import luigi

from .bookings import BookingsToDb
from .customers import CustomersToDb, GomusToCustomerMappingToDb
from .daily_entries import DailyEntriesToDb, ExpectedDailyEntriesToDb
from .exhibitions import ExhibitionTimesToDb
from .events import EventsToDb
from .order_contains import OrderContainsToDb
from .orders import OrdersToDb
from .quotas import QuotasToDb


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
        yield QuotasToDb()

        if not self.light_mode:
            yield BookingsToDb()
            yield CustomersToDb()
            yield EventsToDb()
            yield GomusToCustomerMappingToDb()
            yield OrderContainsToDb()
            yield OrdersToDb()

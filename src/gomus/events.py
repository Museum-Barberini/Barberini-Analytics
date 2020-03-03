import csv

import luigi
import pandas as pd
import psycopg2
from luigi.format import UTF8
from xlrd import xldate_as_datetime

from csv_to_db import CsvToDb
<<<<<<< HEAD:src/gomus/events.py
from set_db_connection_options import set_db_connection_options

from ._utils.extract_bookings import hash_booker_id
from ._utils.fetch_report import FetchEventReservations, FetchGomusReport
from .bookings import BookingsToDB

=======
from gomus._utils.extract_bookings import hash_booker_id
from gomus._utils.fetch_report import FetchEventReservations
from gomus.bookings import BookingsToDB
from set_db_connection_options import set_db_connection_options

>>>>>>> master:src/gomus/events.py

class EventsToDB(CsvToDb):
    table = 'gomus_event'

    columns = [
        ('id', 'INT'),
        ('customer_id', 'INT'),
        ('booking_id', 'INT'),
        ('reservation_count', 'INT'),
        ('order_date', 'DATE'),
        ('status', 'TEXT'),
        ('category', 'TEXT')
    ]

    primary_key = 'id'

    def requires(self):
        return ExtractEventData(columns=[col[0] for col in self.columns])


class ExtractEventData(luigi.Task):
    columns = luigi.parameter.ListParameter(description="Column names")
    seed = luigi.parameter.IntParameter(
        description="Seed to use for hashing", default=666)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events_df = None
        self.categories = [
            'Öffentliche Führung',
            'Event',
            'Gespräch',
            'Kinder-Workshop',
            'Konzert',
            'Lesung',
            'Vortrag']

    def requires(self):
        for category in self.categories:
            yield EnsureBookingsIsRun(category)

    def output(self):
        return luigi.LocalTarget('output/gomus/events.csv', format=UTF8)

    def run(self):
        self.events_df = pd.DataFrame(columns=self.columns)
        for index, event_data in enumerate(self.input()):
            category = self.categories[index]
            for i in range(0, len(event_data), 2):
                self.append_event_data(i, event_data, 'Gebucht', category)
                self.append_event_data(
                    i + 1, event_data, 'Storniert', category)

        with self.output().open('w') as output_csv:
            self.events_df.to_csv(output_csv, index=False)

    def append_event_data(self, index, event_data, status, category):
        with event_data[index].open('r') as sheet:
            sheet_reader = csv.reader(sheet)
            event_id = int(float(next(sheet_reader)[0]))

        event_df = pd.read_csv(event_data[index].path, skiprows=5)
        event_df['Status'] = status
        event_df['Event_id'] = event_id
        event_df['Kategorie'] = category
        event_df = event_df.filter([
            'Id', 'E-Mail', 'Event_id', 'Plätze',
            'Datum', 'Status', 'Kategorie'])

        event_df.columns = self.columns

        event_df['id'] = event_df['id'].apply(int)
        event_df['customer_id'] = event_df['customer_id'].apply(
            hash_booker_id, args=(self.seed,))
        event_df['reservation_count'] = event_df['reservation_count'].apply(
            int)
        event_df['order_date'] = event_df['order_date'].apply(
            self.float_to_datetime)

        self.events_df = self.events_df.append(event_df)

    def float_to_datetime(self, string):
        return xldate_as_datetime(float(string), 0).date()


class EnsureBookingsIsRun(luigi.Task):
    category = luigi.parameter.Parameter(
        description="Category to search bookings for")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
        self.output_list = []
        self.is_complete = False
        self.row_list = []

    def run(self):
        # Enforce the generator to evaluate completely so the task is
        # marked as complete before returning new dependencies
        # LATEST TODO: Re-run pipeline and see whether this works.
        return [dep for dep in list(self._run())]

    def _run(self):
        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()
            query = (f'SELECT booking_id FROM gomus_booking WHERE category=\''
                     f'{self.category}\'')
            cur.execute(query)

            row = cur.fetchone()
            while row is not None:
                event_id = row[0]
                if event_id not in self.row_list:
                    approved = yield FetchEventReservations(event_id, 0)
                    cancelled = yield FetchEventReservations(event_id, 1)
                    self.output_list.append(approved)
                    self.output_list.append(cancelled)
                    self.row_list.append(event_id)
                row = cur.fetchone()
            self.is_complete = True

        finally:
            if conn is not None:
                conn.close()

    def output(self):
        return self.output_list

    def complete(self):
        return self.is_complete

    def requires(self):
        yield BookingsToDB()

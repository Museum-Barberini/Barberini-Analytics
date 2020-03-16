import csv

import luigi
import pandas as pd
import psycopg2
from luigi.format import UTF8
from xlrd import xldate_as_datetime

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from gomus._utils.fetch_report import FetchEventReservations
from gomus.bookings import BookingsToDB
from set_db_connection_options import set_db_connection_options


class EventsToDB(CsvToDb):
    table = 'gomus_event'

    columns = [
        ('event_id', 'INT'),
        ('booking_id', 'INT'),
        ('reservation_count', 'INT'),
        ('order_date', 'DATE'),
        ('status', 'TEXT'),
        ('category', 'TEXT')
    ]

    primary_key = 'event_id'

    foreign_keys = [
        {
            'origin_column': 'booking_id',
            'target_table': 'gomus_booking',
            'target_column': 'booking_id'
        }
    ]

    def requires(self):
        return ExtractEventData(
            columns=[col[0] for col in self.columns],
            foreign_keys=self.foreign_keys)


class ExtractEventData(DataPreparationTask):
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

    def _requires(self):
        return luigi.task.flatten([
            BookingsToDB(),
            super()._requires()
        ])

    def requires(self):
        for category in self.categories:
            yield EnsureBookingsIsRun(category)

    def output(self):
        return luigi.LocalTarget('output/gomus/events.csv', format=UTF8)

    def run(self):
        self.events_df = pd.DataFrame(columns=self.columns)

        # for every kind of category
        for index, event_files in enumerate(self.input()):
            category = self.categories[index]
            with event_files.open('r') as events:

                # for every event that falls into that category
                for i, path in enumerate(events):
                    path = path.replace('\n', '')

                    # handle booked and cancelled events
                    event_data = luigi.LocalTarget(path, format=UTF8)
                    if i % 2 == 0:
                        self.append_event_data(event_data, 'Gebucht', category)
                    else:
                        self.append_event_data(event_data,
                                               'Storniert',
                                               category)

        self.events_df = self.ensure_foreign_keys(self.events_df)

        with self.output().open('w') as output_csv:
            self.events_df.to_csv(output_csv, index=False)

    def append_event_data(self, event_data, status, category):
        with event_data.open('r') as sheet:
            sheet_reader = csv.reader(sheet)
            event_id = int(float(next(sheet_reader)[0]))

        event_df = pd.read_csv(event_data.path, skiprows=5)
        event_df['Status'] = status
        event_df['Event_id'] = event_id
        event_df['Kategorie'] = category
        event_df = event_df.filter([
            'Id', 'E-Mail', 'Event_id', 'Plätze',
            'Datum', 'Status', 'Kategorie'])

        event_df.columns = self.columns

        event_df['event_id'] = event_df['event_id'].apply(int)
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
        self.row_list = []

    def run(self):
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
                    self.output_list.append(approved.path)
                    self.output_list.append(cancelled.path)
                    self.row_list.append(event_id)
                row = cur.fetchone()

            # write list of all event reservation to output file
            with self.output().open('w') as all_outputs:
                all_outputs.write('\n'.join(self.output_list))

        finally:
            if conn is not None:
                conn.close()

    # save a list of paths for all single csv files
    def output(self):
        cat = self.cleanse_umlauts(self.category)
        return luigi.LocalTarget(f'output/gomus/all_{cat}_reservations.txt',
                                 format=UTF8)

    def requires(self):
        yield BookingsToDB()

    # this function should not have to exist, but luigi apparently
    # can't deal with UTF-8 symbols in their target paths
    def cleanse_umlauts(self, string):
        return string.translate(string.maketrans({
            'Ä': 'Ae', 'ä': 'ae',
            'Ö': 'Oe', 'ö': 'oe',
            'Ü': 'Ue', 'ü': 'ue'}))

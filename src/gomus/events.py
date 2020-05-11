import csv
import datetime as dt

import luigi
import pandas as pd
from luigi.format import UTF8
from xlrd import xldate_as_datetime

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from gomus._utils.fetch_report import FetchEventReservations
from gomus.bookings import BookingsToDB
from gomus.customers import hash_id


class EventsToDB(CsvToDb):

    table = 'gomus_event'

    def requires(self):
        return ExtractEventData(
            columns=[col[0] for col in self.columns],
            table=self.table)


class ExtractEventData(DataPreparationTask):
    columns = luigi.parameter.ListParameter(description="Column names")
    seed = luigi.parameter.IntParameter(
        description="Seed to use for hashing", default=666)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.events_df = None
        self.categories = [
            "Öffentliche Führung",
            "Event",
            "Gespräch",
            "Kinder-Workshop",
            "Konzert",
            "Lesung",
            "Vortrag"]

    def _requires(self):
        return luigi.task.flatten([
            BookingsToDB(),
            super()._requires()
        ])

    def requires(self):
        for category in self.categories:
            yield FetchCategoryReservations(category=category)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/events.csv',
            format=UTF8
        )

    def run(self):
        self.events_df = pd.DataFrame(columns=self.columns)

        # for every kind of category
        for index, event_files in enumerate(self.input()):
            category = self.categories[index]
            with event_files.open('r') as events:
                # for every event that falls into that category
                for i, path in enumerate(events):
                    path = path.replace('\n', '')
                    if not path:
                        continue

                    # handle booked and cancelled events
                    event_data = luigi.LocalTarget(path, format=UTF8)
                    self.append_event_data(
                        event_data,
                        "Storniert" if i % 2 else "Gebucht",
                        category)

        self.events_df = self.ensure_foreign_keys(self.events_df)

        with self.output().open('w') as output_csv:
            self.events_df.to_csv(output_csv, index=False)

    def append_event_data(self, event_data, status, category):
        with event_data.open('r') as sheet:
            sheet_reader = csv.reader(sheet)
            try:
                event_id = int(float(next(sheet_reader)[0]))
            except StopIteration:
                event_id = None

        if event_id:
            event_df = pd.read_csv(event_data.path, skiprows=5)
            event_df['Status'] = status
            event_df['Event_id'] = event_id
            event_df['Kategorie'] = category
            event_df = event_df.filter([
                "Id", "Event_id", "E-Mail", "Plätze",
                "Datum", "Status", "Kategorie"])

            event_df.columns = self.columns

            event_df['event_id'] = event_df['event_id'].apply(int)
            event_df['customer_id'] = event_df['customer_id'].apply(hash_id)
            event_df['reservation_count'] = event_df[
                'reservation_count'].apply(int)
            event_df['order_date'] = event_df['order_date'].apply(
                self.float_to_datetime)

            self.events_df = self.events_df.append(event_df)

    def float_to_datetime(self, string):
        return xldate_as_datetime(float(string), 0).date()


class FetchCategoryReservations(DataPreparationTask):
    category = luigi.parameter.Parameter(
        description="Category to search bookings for")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_list = []
        self.row_list = []

    def run(self):

        if self.minimal_mode:
            query = f'''
                SELECT booking_id FROM gomus_booking
                WHERE category='{self.category}'
                ORDER BY start_datetime DESC LIMIT 2
            '''
        else:
            two_weeks_ago = dt.datetime.today() - dt.timedelta(weeks=2)
            query = (f'SELECT booking_id FROM gomus_booking WHERE '
                     f'category=\'{self.category}\' '
                     f'AND start_datetime > \'{two_weeks_ago}\'')

        booking_ids = self.db_connector.query(query)

        for row in booking_ids:
            event_id = row[0]
            if event_id not in self.row_list:
                approved = FetchEventReservations(
                    booking_id=event_id,
                    status=0)
                yield approved
                cancelled = FetchEventReservations(
                    booking_id=event_id,
                    status=1)
                yield cancelled
                if approved and cancelled:
                    self.output_list.append(approved.output().path)
                    self.output_list.append(cancelled.output().path)
                self.row_list.append(event_id)

        # write list of all event reservation to output file
        with self.output().open('w') as all_outputs:
            all_outputs.write('\n'.join(self.output_list) + '\n')

    # save a list of paths for all single csv files
    def output(self):
        cat = cleanse_umlauts(self.category)
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/all_{cat}_reservations.txt',
            format=UTF8
        )

    def requires(self):
        yield BookingsToDB()


# this function should not have to exist, but luigi apparently
# can't deal with UTF-8 symbols in their target paths
def cleanse_umlauts(string):
    return string.translate(string.maketrans({
        'Ä': 'Ae', 'ä': 'ae',
        'Ö': 'Oe', 'ö': 'oe',
        'Ü': 'Ue', 'ü': 'ue'}))

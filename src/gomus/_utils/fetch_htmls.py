from copy import copy
import datetime as dt
import os
import time

import luigi
import pandas as pd
import requests

from _utils import DataPreparationTask
from ..orders import OrdersToDb
from .extract_bookings import ExtractGomusBookings


class FailableTarget:
    """
    Decorator for luigi.LocalTarget to mark a target as failed.

    This allows a task to complete despite a failure occured during execution.
    """

    def __init__(self, task):

        super().__init__()
        self.task = task

    def __getattribute__(self, name):

        try:
            return super().__getattribute__(name)
        except AttributeError:
            return self.task.__getattribute__(name)

    @property
    def is_error(self):

        return self.path.endswith('.error')

    def has_error(self):

        if self.is_error:
            return True
        return self.as_error().exists()

    def exists(self):

        if self.task.exists():
            return True
        if self.is_error:
            return False
        return self.as_error().exists()

    def as_error(self):

        if self.is_error:
            return self
        error_task = copy(self.task)
        error_task.path = f'{error_task.path}.error'
        return type(self)(error_task)



class FetchGomusHTML(DataPreparationTask):

    base_url = luigi.Parameter(
        description="The base URL of the gomus server",
        default="https://barberini.gomus.de")

    url = luigi.Parameter(description="The URL to fetch")

    ignored_status_codes = luigi.ListParameter(
        description="HTTP status codes for that an error should not be raised",
        default=[])

    def output(self):

        name = f'{self.output_dir}/gomus/html/' + \
            self.url. \
            replace('/', '_'). \
            replace('.', '_'). \
            replace('?', '_'). \
            replace('&', '_') + \
            '.html'

        return FailableTarget(
            luigi.LocalTarget(name, format=luigi.format.Nop))

    def run(self):

        # polite get: we don't want to overwhelm the server
        time.sleep(0.2)

        output = self.output()

        response = requests.get(
            self.base_url + self.url,
            cookies=dict(
                _session_id=os.environ['GOMUS_SESS_ID']),
            stream=True)
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            if error.response.status_code not in self.ignored_status_codes:
                raise
            else:
                output = output.as_error()

        with output.open('wb') as html_out:
            for block in response.iter_content(1024):
                html_out.write(block)


class FetchBookingsHTML(DataPreparationTask):

    timespan = luigi.parameter.Parameter(default='_nextYear')
    columns = luigi.parameter.ListParameter(description="Column names")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_list = []

    def requires(self):
        return ExtractGomusBookings(
            timespan=self.timespan, columns=self.columns)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/bookings_htmls.txt')

    def run(self):
        with self.input().open('r') as input_file:
            bookings = pd.read_csv(input_file)

            if self.minimal_mode:
                bookings = bookings.head(5)

        today_time = dt.datetime.today() - dt.timedelta(weeks=5)
        db_booking_rows = self.db_connector.query(f'''
            SELECT booking_id FROM gomus_booking
            WHERE start_datetime < '{today_time}'
        ''')

        for i, row in bookings.iterrows():
            booking_id = row['booking_id']

            booking_in_db = False
            for db_row in db_booking_rows:
                if db_row[0] == booking_id:
                    booking_in_db = True
                    break

            if not booking_in_db:
                html_target = yield FetchGomusHTML(
                    url=f'/admin/bookings/{booking_id}')
                self.output_list.append(html_target.path)

        with self.output().open('w') as html_files:
            html_files.write('\n'.join(self.output_list))


class FetchOrdersHTML(DataPreparationTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_list = []
        self.order_ids = []

    def requires(self):
        return OrdersToDb()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/orders_htmls.txt')

    def get_order_ids(self):

        order_ids = []

        query_limit = 'LIMIT 10' if self.minimal_mode else ''

        order_ids = self.db_connector.query(f'''
            SELECT a.order_id
            FROM gomus_order AS a
            LEFT OUTER JOIN gomus_order_contains AS b
            ON a.order_id = b.order_id
            WHERE ticket IS NULL
            {query_limit}
        ''')

        return order_ids

    def run(self):
        self.order_ids = [order_id[0] for order_id in self.get_order_ids()]

        for i in range(len(self.order_ids)):
            html_target = yield FetchGomusHTML(
                url=f'/admin/orders/{self.order_ids[i]}')
            self.output_list.append(html_target.path)

        with self.output().open('w') as html_files:
            html_files.write('\n'.join(self.output_list))

import datetime as dt
import luigi
import numpy as np
import pandas as pd
import psycopg2
from luigi.format import UTF8
from xlrd import xldate_as_datetime

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from gomus._utils.fetch_report import FetchGomusReport
from gomus.customers import GomusToCustomerMappingToDB

COLUMNS = [
    ('order_id', 'INT'),
    ('order_date', 'DATE'),
    ('customer_id', 'INT'),
    ('valid', 'BOOL'),
    ('paid', 'BOOL'),
    ('origin', 'TEXT')
]


class OrdersToDB(CsvToDb):
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())
    table = 'gomus_order'

    columns = COLUMNS

    primary_key = 'order_id'

    foreign_keys = [
        {
            'origin_column': 'customer_id',
            'target_table': 'gomus_customer',
            'target_column': 'customer_id'
        }
    ]

    def requires(self):
        return ExtractOrderData(
            foreign_keys=self.foreign_keys,
            today=self.today)


class ExtractOrderData(DataPreparationTask):
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = [col[0] for col in COLUMNS]

    def _requires(self):
        return luigi.task.flatten([
            GomusToCustomerMappingToDB(),
            super()._requires()
        ])

    def requires(self):
        return FetchGomusReport(report='orders',
                                suffix='_7days',
                                today=self.today)

    def output(self):
        return luigi.LocalTarget('output/gomus/orders.csv', format=UTF8)

    def run(self):
        with next(self.input()).open('r') as input_csv:
            df = pd.read_csv(input_csv)
        if df.empty:
            df = pd.DataFrame(columns=[
                                    'order_id',
                                    'order_date',
                                    'customer_id',
                                    'valid',
                                    'paid',
                                    'origin'])
        else:
            df = df.filter([
                'Bestellnummer', 'Erstellt', 'Kundennummer',
                'ist gültig?', 'Bezahlstatus', 'Herkunft'
            ])
            df.columns = self.columns

            df['order_id'] = df['order_id'].apply(int)
            df['order_date'] = df['order_date'].apply(self.float_to_datetime)
            df['customer_id'] = df['customer_id'].apply(
                self.query_customer_id).astype('Int64')
            df['valid'] = df['valid'].apply(self.parse_boolean, args=('Ja',))
            df['paid'] = df['paid'].apply(
                self.parse_boolean,
                args=('bezahlt',))

            df = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_csv:
            df.to_csv(output_csv, index=False, header=True)

    def float_to_datetime(self, string):
        return xldate_as_datetime(float(string), 0).date()

    def query_customer_id(self, customer_string):
        if np.isnan(customer_string):
            return 0
            # if the customer_string is NaN, we set the customer_id to 0
        else:
            org_id = int(float(customer_string))
        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )

            cur = conn.cursor()
            query = (f'SELECT customer_id FROM gomus_to_customer_mapping '
                     f'WHERE gomus_id = {org_id}')
            cur.execute(query)

            customer_row = cur.fetchone()
            if customer_row is not None:
                customer_id = customer_row[0]
            else:
                customer_id = np.nan
                # if we can't find the customer_id, but it isn't NaN,
                # we set the customer_id to NaN

        finally:
            if conn is not None:
                conn.close()
        return customer_id

    def parse_boolean(self, string, bool_string):
        return string.lower() == bool_string.lower()

#!/usr/bin/env python3
import luigi
import numpy as np
import pandas as pd
import psycopg2
from luigi.format import UTF8
from xlrd import xldate_as_datetime

from csv_to_db import CsvToDb
from gomus._utils.fetch_report import FetchGomusReport
from gomus.customers import CustomersToDB
from set_db_connection_options import set_db_connection_options


class OrdersToDB(CsvToDb):
    table = 'gomus_order'

    columns = [
        ('order_id', 'INT'),
        ('order_date', 'DATE'),
        ('customer_id', 'INT'),
        ('valid', 'BOOL'),
        ('paid', 'BOOL'),
        ('origin', 'TEXT')
    ]

    primary_key = 'order_id'

    foreign_keys = [
        {
            "origin_column": "customer_id",
            "target_table": "gomus_customer",
            "target_column": "customer_id"
        }
    ]

    def requires(self):
        return ExtractOrderData(columns=[col[0] for col in self.columns])


class ExtractOrderData(luigi.Task):
    columns = luigi.parameter.ListParameter(description="Column names")

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def _requires(self):
        return luigi.task.flatten([
            CustomersToDB(),
            super()._requires()
        ])

    def requires(self):
        return FetchGomusReport(report='orders', suffix='_1day')

    def output(self):
        return luigi.LocalTarget('output/gomus/orders.csv', format=UTF8)

    def run(self):
        with next(self.input()).open('r') as input_csv:
            df = pd.read_csv(input_csv)

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
        df['paid'] = df['paid'].apply(self.parse_boolean, args=('bezahlt',))

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
            query = (f'SELECT customer_id FROM gomus_customer WHERE '
                     f'gomus_id = {org_id}')
            cur.execute(query)

            customer_row = cur.fetchone()
            if customer_row is not None:
                customer_id = customer_row[0]
            else:
                customer_id = np.nan
                # if we can't find the customer_id, but it isn't NaN,
                # we set the customer_id to NaN
        except psycopg2.DatabaseError as error:
            print(error)
            exit(1)
        finally:
            if conn is not None:
                conn.close()
        return customer_id

    def parse_boolean(self, string, bool_string):
        return string.lower() == bool_string.lower()

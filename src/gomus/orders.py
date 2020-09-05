"""Provides tasks for downloading all gomus orders into the database."""

import datetime as dt

import luigi
from luigi.format import UTF8
import numpy as np
import pandas as pd
from xlrd import xldate_as_datetime

from _utils import CsvToDb, DataPreparationTask
from ._utils.fetch_report import FetchGomusReport
from .customers import GomusToCustomerMappingToDb


class OrdersToDb(CsvToDb):
    table = 'gomus_order'

    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())

    def requires(self):
        return ExtractOrderData(
            table=self.table,
            columns=[col[0] for col in self.columns],
            today=self.today)


class ExtractOrderData(DataPreparationTask):
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())
    columns = luigi.parameter.ListParameter(description="Column names")

    def _requires(self):
        return luigi.task.flatten([
            GomusToCustomerMappingToDb(),
            super()._requires()
        ])

    def requires(self):
        suffix = '_1day' if self.minimal_mode else '_7days'
        return FetchGomusReport(report='orders',
                                suffix=suffix,
                                today=self.today)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/orders.csv',
            format=UTF8
        )

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
                "Bestellnummer", "Erstellt", "Kundennummer",
                "ist gültig?", "Bezahlstatus", "Herkunft"
            ])
            df.columns = self.columns

            df['order_id'] = df['order_id'].apply(int)
            df['order_date'] = df['order_date'].apply(self.float_to_datetime)
            df['customer_id'] = df['customer_id'].apply(
                self.query_customer_id).astype('Int64')
            df['valid'] = df['valid'].apply(self.parse_boolean, args=("Ja",))
            df['paid'] = df['paid'].apply(
                self.parse_boolean,
                args=("bezahlt",))

        df = self.filter_fkey_violations(df)

        with self.output().open('w') as output_csv:
            df.to_csv(output_csv, index=False, header=True)

    def float_to_datetime(self, string):
        return xldate_as_datetime(float(string), 0).date()

    def query_customer_id(self, customer_string):
        if np.isnan(customer_string):
            return 0

        org_id = int(float(customer_string))
        customer_row = self.db_connector.query(
            f'''
                SELECT customer_id FROM gomus_to_customer_mapping
                WHERE gomus_id = {org_id}
            ''',
            only_first=True)
        customer_id = customer_row[0] if customer_row else np.nan
        return customer_id

    def parse_boolean(self, string, bool_string):
        return string.lower() == bool_string.lower()

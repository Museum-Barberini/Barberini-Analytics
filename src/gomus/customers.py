#!/usr/bin/env python3
import datetime as dt
import luigi
import pandas as pd
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation import DataPreparationTask

from gomus._utils.cleanse_data import CleansePostalCodes
from gomus._utils.extract_customers import hash_id
from gomus._utils.fetch_report import FetchGomusReport


class CustomersToDb(CsvToDb):

    amount = luigi.parameter.Parameter(default='regular')
    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    table = 'gomus_customer'

    def requires(self):
        return CleansePostalCodes(
            amount=self.amount,
            columns=[col[0] for col in self.columns],
            today=self.today)

    def read_csv(self, input_csv):
        df = super().read_csv(input_csv)
        # This is necessary to prevent pandas from adding
        # '.0' to some postal codes
        df['cleansed_postal_code'] = \
            df['cleansed_postal_code'].apply(str)
        df['cleansed_postal_code'] = \
            df['cleansed_postal_code'].str.replace('.0', '')
        return df


class GomusToCustomerMappingToDb(CsvToDb):

    table = 'gomus_to_customer_mapping'

    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    def requires(self):
        return ExtractGomusToCustomerMapping(
            columns=[col[0] for col in self.columns],
            table=self.table,
            today=self.today)


class ExtractGomusToCustomerMapping(DataPreparationTask):
    columns = luigi.parameter.ListParameter(description="Column names")
    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    def _requires(self):
        return luigi.task.flatten([
            CustomersToDb(today=self.today),
            super()._requires()
        ])

    def requires(self):
        suffix = '_1day' if self.minimal_mode else '_7days'

        return FetchGomusReport(report='customers',
                                today=self.today,
                                suffix=suffix)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/gomus_to_customers_mapping.csv',
            format=UTF8
        )

    def run(self):
        with next(self.input()).open('r') as input_csv:
            df = pd.read_csv(input_csv)

        df = df.filter(['Nummer', 'E-Mail'])
        df.columns = self.columns

        df['gomus_id'] = df['gomus_id'].apply(int)
        df['customer_id'] = df.apply(
            lambda x: hash_id(
                x['customer_id'], alternative=x['gomus_id']
            ), axis=1)

        df = self.filter_fkey_violations(df)

        with self.output().open('w') as output_csv:
            df.to_csv(output_csv, index=False, header=True)

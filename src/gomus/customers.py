#!/usr/bin/env python3
import datetime as dt
import luigi
import mmh3
import os
import pandas as pd
import numpy as np
from luigi.format import UTF8

from csv_to_db import CsvToDb
from data_preparation_task import DataPreparationTask
from set_db_connection_options import set_db_connection_options

from gomus._utils.fetch_report import FetchGomusReport


class CustomersToDB(CsvToDb):

    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    table = 'gomus_customer'

    columns = [
        ('customer_id', 'INT'),
        ('postal_code', 'TEXT'),  # e.g. non-german
        ('newsletter', 'BOOL'),
        ('gender', 'TEXT'),
        ('category', 'TEXT'),
        ('language', 'TEXT'),
        ('country', 'TEXT'),
        ('type', 'TEXT'),  # shop, shop guest or normal
        ('register_date', 'DATE'),
        ('annual_ticket', 'BOOL'),
        ('valid_mail', 'BOOL')
    ]

    primary_key = 'customer_id'

    def requires(self):
        return ExtractCustomerData(
            columns=[col[0] for col in self.columns],
            today=self.today)


class GomusToCustomerMappingToDB(CsvToDb):

    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    table = 'gomus_to_customer_mapping'

    columns = [
        ('gomus_id', 'INT'),
        ('customer_id', 'INT')
    ]

    primary_key = 'customer_id'

    foreign_keys = [
        {
            'origin_column': 'customer_id',
            'target_table': 'gomus_customer',
            'target_column': 'customer_id'
        }
    ]

    def requires(self):
        return ExtractGomusToCustomerMapping(
            columns=[col[0] for col in self.columns],
            foreign_keys=self.foreign_keys, today=self.today)


class ExtractCustomerData(DataPreparationTask):
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        suffix = '_1day' if os.environ['MINIMAL'] == 'True' else '_7days'

        return FetchGomusReport(report='customers',
                                today=self.today,
                                suffix=suffix)

    def output(self):
        return luigi.LocalTarget('output/gomus/customers.csv', format=UTF8)

    def run(self):
        with next(self.input()).open('r') as input_csv:
            df = pd.read_csv(input_csv)

        df['Gültige E-Mail'] = df['E-Mail'].apply(isinstance, args=(str,))

        # Insert Hash of E-Mail into E-Mail field,
        # or original ID if there is none
        df['E-Mail'] = df.apply(
            lambda x: hash_id(
                x['E-Mail'], alternative=x['Nummer']
            ), axis=1)

        df = df.filter([
            'E-Mail', 'PLZ',
            'Newsletter', 'Anrede', 'Kategorie',
            'Sprache', 'Land', 'Typ',
            'Erstellt am', 'Jahreskarte', 'Gültige E-Mail'])

        df.columns = self.columns

        df['postal_code'] = df['postal_code'].apply(self.cut_decimal_digits)
        df['newsletter'] = df['newsletter'].apply(self.parse_boolean)
        df['gender'] = df['gender'].apply(self.parse_gender)
        df['register_date'] = pd.to_datetime(
            df['register_date'], format='%d.%m.%Y')
        df['annual_ticket'] = df['annual_ticket'].apply(self.parse_boolean)

        # Drop duplicate occurences of customers with same mail,
        # keeping the most recent one
        df = df.drop_duplicates(subset=['customer_id'], keep='last')

        with self.output().open('w') as output_csv:
            df.to_csv(output_csv, index=False, header=True)

    def parse_boolean(self, string):
        return string == 'ja'

    def parse_gender(self, string):
        if string == 'Frau':
            return 'w'
        elif string == 'Herr':
            return 'm'
        return ''

    def cut_decimal_digits(self, post_string):
        if post_string is np.nan:
            post_string = ''
        post_string = str(post_string)
        if len(post_string) >= 2:
            return post_string[:-2] if post_string[-2:] == '.0' else \
                post_string


class ExtractGomusToCustomerMapping(DataPreparationTask):
    columns = luigi.parameter.ListParameter(description="Column names")
    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    def _requires(self):
        return luigi.task.flatten([
            CustomersToDB(),
            super()._requires()
        ])

    def requires(self):
        suffix = '_1day' if os.environ['MINIMAL'] == 'True' else '_7days'

        return FetchGomusReport(report='customers',
                                today=self.today,
                                suffix=suffix)

    def output(self):
        return luigi.LocalTarget('output/gomus/gomus_to_customers_mapping.csv',
                                 format=UTF8)

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

        df = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_csv:
            df.to_csv(output_csv, index=False, header=True)


# Return hash for e-mail value, or alternative (usually original gomus_id
# or default value 0 for the dummy customer) if the e-mail is invalid
def hash_id(email, alternative=0, seed=666):
    if not isinstance(email, str):
        return int(float(alternative))
    return mmh3.hash(email, seed, signed=True)

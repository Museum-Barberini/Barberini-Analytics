#!/usr/bin/env python3
import datetime as dt
import luigi
import mmh3
import pandas as pd
import numpy as np
from luigi.format import UTF8

from data_preparation import DataPreparationTask

from gomus._utils.fetch_report import FetchGomusReport


class ExtractCustomerData(DataPreparationTask):

    table = 'gomus_customer'
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        suffix = '_1day' if self.minimal_mode else '_7days'

        return FetchGomusReport(report='customers',
                                today=self.today,
                                suffix=suffix)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/customers.csv',
            format=UTF8
        )

    def run(self):
        with next(self.input()).open('r') as input_csv:
            df = pd.read_csv(input_csv)

        df['Gültige E-Mail'] = df['E-Mail'].apply(isinstance, args=(str,))

        df['Bereinigte PLZ'] = None
        df['Bereinigtes Land'] = None

        # Check whether email contains interesting tags
        df['Tourismus Tags'] = df['E-Mail'].apply(self.check_mail)

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
            'Erstellt am', 'Jahreskarte',
            'Gültige E-Mail', 'Bereinigte PLZ',
            'Bereinigtes Land', 'Tourismus Tags'])

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

    def check_mail(self, mail):
        tourism_tags = [
            'reise', 'kultur', 'freunde', 'förder', 'foerder',
            'guide', 'hotel', 'travel', 'event', 'visit',
            'verein', 'stiftung'
        ]

        contained_tags = []

        if not pd.isnull(mail):
            for tag in tourism_tags:
                if tag in mail:
                    contained_tags.append(tag)

        return contained_tags


# Return hash for e-mail value, or alternative (usually original gomus_id
# or default value 0 for the dummy customer) if the e-mail is invalid
def hash_id(email, alternative=0, seed=666):
    if not isinstance(email, str):
        return int(float(alternative))

    return mmh3.hash(email, seed, signed=True)

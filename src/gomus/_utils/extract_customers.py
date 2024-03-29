#!/usr/bin/env python3
import datetime as dt
import os

import luigi
import mmh3
import pandas as pd
import numpy as np
from luigi.format import UTF8

from _utils import DataPreparationTask, logger
from .fetch_report import FetchGomusReport


class ExtractCustomerData(DataPreparationTask):

    table = 'gomus_customer'
    today = luigi.parameter.DateParameter(
        default=dt.datetime.today())
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        suffix = '_1day' if self.minimal_mode else \
            f'_{os.getenv("GOMUS_HISTORIC_DAYS", 7)}days'

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

        # Will be filled during CleansePostalCodes
        df['Bereinigte PLZ'] = None
        df['Bereinigtes Land'] = None

        # Check whether email contains interesting tags
        df['Tourismus Tags'] = df['E-Mail'].apply(self.check_mail)

        # Prepare columns for latitude and longitude
        df['Breitengrad'] = None
        df['Längengrad'] = None

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
            'Erstellt am',
            'Gültige E-Mail', 'Bereinigte PLZ',
            'Bereinigtes Land', 'Tourismus Tags',
            'Breitengrad', 'Längengrad'
            ])
        # ct: Jahreskarten are no longer available in the customer report. We
        # could fetch them through the new Jahreskarteninhabendenreport or try
        # to derive them from the ticket table. For now, we defer this because
        # the old values in the existing table are implausible anyway.
        # insert this column after 'Erstellt am'
        df.insert(9, 'Jahreskarte', None)

        df.columns = self.columns

        df['postal_code'] = df['postal_code'].apply(self.cut_decimal_digits)
        df['newsletter'] = df['newsletter'].apply(self.parse_boolean)
        df['gender'] = df['gender'].apply(self.parse_gender)

        # Sometimes columns are shifted in exports -> exclude them for now
        df['register_date'] = pd.to_datetime(
            df['register_date'], format='%d.%m.%Y', errors='coerce')
        invalid_dates = df[df['register_date'].isna()]
        invalid_count = len(invalid_dates)
        if invalid_count:
            logger.warning(
                f'Found {len(invalid_dates)} invalid dates in report. '
                f'Dropping them.')
            if 1 - invalid_count / len(df) < 0.5:
                raise ValueError("Too many invalid dates! Aborting.")
            df = df[df['register_date'] != pd.NaT]

        df['tourism_tags'] = df['tourism_tags'].fillna('[]')

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

        if pd.isnull(mail):
            return []

        return [tag for tag in tourism_tags if tag in mail]


def hash_id(email, alternative=0, seed=666):
    """Hash the given email address for privacy. Use alternative if empty."""
    if not isinstance(email, str):
        return int(float(alternative))

    return mmh3.hash(email, seed, signed=True)

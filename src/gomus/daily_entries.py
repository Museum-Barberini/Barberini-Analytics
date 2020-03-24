#!/usr/bin/env python3
import datetime as dt

import luigi
import numpy as np
import pandas as pd
from luigi.format import UTF8

from csv_to_db import CsvToDb

from data_preparation_task import DataPreparationTask
from gomus._utils.fetch_report import FetchGomusReport


class AbstractDailyEntriesToDB(CsvToDb):
    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    columns = [
        ('id', 'INT'),
        ('ticket', 'TEXT'),
        ('datetime', 'TIMESTAMP'),
        ('count', 'INT'),
    ]

    primary_key = ('id', 'datetime')


class DailyEntriesToDB(AbstractDailyEntriesToDB):
    table = 'gomus_daily_entry'

    def requires(self):
        return ExtractDailyEntryData(expected=False,
                                     columns=self.columns,
                                     today=self.today)


class ExpectedDailyEntriesToDB(AbstractDailyEntriesToDB):
    table = 'gomus_expected_daily_entry'

    def requires(self):
        return ExtractDailyEntryData(expected=True,
                                     columns=self.columns,
                                     today=self.today)


class ExtractDailyEntryData(DataPreparationTask):
    today = luigi.parameter.DateParameter(default=dt.datetime.today())
    expected = luigi.parameter.BoolParameter(
        description="Whether to return actual or expected entries")
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return FetchGomusReport(
            report='entries', suffix='_1day', sheet_indices=[
                0, 1] if not self.expected else [
                2, 3], today=self.today)

    def output(self):
        return luigi.LocalTarget(
            f'output/gomus/{"expected_" if self.expected else ""}'
            'daily_entries.csv', format=UTF8)

    def run(self):
        # get date from first sheet
        inputs = self.input()
        with next(inputs).open('r') as first_sheet:
            while True:
                try:
                    date_line = first_sheet.readline()
                    date = pd.to_datetime(date_line.split(',')[2],
                                          format='"%d.%m.%Y"')
                    break
                except ValueError:
                    continue

        # get remaining data from second sheet
        with next(inputs).open('r') as second_sheet:
            df = pd.read_csv(second_sheet, skipfooter=1, engine='python')
            entries_df = pd.DataFrame(columns=[col[0] for col in self.columns])

            for index, row in df.iterrows():
                for i in range(24):
                    row_index = index * 24 + i
                    entries_df.at[row_index, 'id'] = int(
                        np.nan_to_num(row['ID']))
                    entries_df.at[row_index, 'ticket'] = row['Ticket']

                    time = dt.time(hour=i)
                    entries_df.at[row_index, 'datetime'] = \
                        dt.datetime.combine(date, time)

                    # handle different hour formats for expected/actual entries
                    if self.expected:
                        count_index = str(i) + ':00'
                    else:
                        count_index = str(float(i))
                    entries_df.at[row_index, 'count'] = self.safe_parse_int(
                        row[count_index])

            with self.output().open('w') as output_csv:
                entries_df.to_csv(output_csv, index=False, header=True)

    def safe_parse_int(self, val):
        return int(np.nan_to_num(val))

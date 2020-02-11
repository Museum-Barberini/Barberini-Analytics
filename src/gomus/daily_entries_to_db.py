#!/usr/bin/env python3
import luigi
from luigi.format import UTF8
import numpy as np
import pandas as pd

from csv_to_db import CsvToDb
from gomus_report import FetchGomusReport

class AbstractDailyEntriesToDB(CsvToDb):
    columns = [
        ('id', 'INT'),
        ('ticket', 'TEXT'),
        ('date', 'DATE'),
        ('count', 'INT'),
        *[(f'"{i}"', 'INT') for i in range(24)]
    ]

    primary_key = ('id', 'date')


class DailyEntriesToDB(AbstractDailyEntriesToDB):
    table = 'gomus_daily_entry'

    def requires(self):
        return ExtractDailyEntryData(expected=False, columns=self.columns)


class ExpectedDailyEntriesToDB(AbstractDailyEntriesToDB):
    table = 'gomus_expected_daily_entry'

    def requires(self):
        return ExtractDailyEntryData(expected=True, columns=self.columns)


class ExtractDailyEntryData(luigi.Task):
    expected = luigi.parameter.BoolParameter(description="Whether to return actual or expected entries")
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return FetchGomusReport(report='entries', suffix='_1day', sheet_indices=[0, 1] if not self.expected else [2, 3], refresh_wait_time=20)

    def output(self):
        return luigi.LocalTarget(f'output/gomus/{"expected_" if self.expected else ""}daily_entries.csv', format=UTF8)

    def run(self):
        # get date from first sheet
        inputs = self.input()
        with next(inputs).open('r') as first_sheet:
            for i in range(4): date_line = first_sheet.readline()
            date = date_line.split(',')[2]
            while date == '""':
                print(date)
                date_line = first_sheet.readline()
                date = date_line.split(',')[2]
            print(date)
        # get remaining data from second sheet
        with next(inputs).open('r') as second_sheet:
            df = pd.read_csv(second_sheet, skipfooter=1, engine='python')
            df.insert(2, 'date', pd.to_datetime(date, format='"%d.%m.%Y"'))
            count = df.pop('Gesamt')
            df.insert(3, 'count', count)

            df.columns = [el[0] for el in self.columns]
            
            df['id'] = df['id'].apply(int)
            df['count'] = df['count'].apply(int)
            for i in range(24):
                df[f'"{i}"'] = df[f'"{i}"'].apply(self.safe_parse_int)

        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False, header=True)

    def safe_parse_int(self, val):
        return int(np.nan_to_num(val))

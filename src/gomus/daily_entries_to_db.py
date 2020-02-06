#!/usr/bin/env python3
import luigi
from luigi.format import UTF8
import numpy as np
import pandas as pd

from csv_to_db import CsvToDb
from gomus_report import FetchGomusReport

_columns = [
    ('id', 'INT'),
    ('ticket', 'TEXT'),
    ('date', 'DATE'),
    ('count', 'INT'),
    *[(f'"{i}"', 'INT') for i in range(24)]
]

_primary_key = ('id', 'date')

class DailyEntriesToDB(CsvToDb):
    table = 'gomus_daily_entry'
    columns = _columns
    primary_key = _primary_key
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    """
    def requires(self):
        return ExtractDailyEntryData(expected=False)

class ExpectedDailyEntriesToDB(CsvToDb):
    table = 'gomus_expected_daily_entry'
    columns = _columns
    primary_key = _primary_key

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return ExtractDailyEntryData(expected=True)

class ExtractDailyEntryData(luigi.Task):
    expected = luigi.parameter.BoolParameter(description="Whether to return actual or expected entries")

    def requires(self):
        return FetchGomusReport(report='entry', suffix='_1day', sheet_indices=[0, 1] if not self.expected else [2, 3], refresh_wait_time=20)

    def output(self):
        if self.expected:
            return luigi.LocalTarget('output/gomus/expected_daily_entries.csv', format=UTF8)
        return luigi.LocalTarget('output/gomus/daily_entries.csv', format=UTF8)

    def run(self):
        # get date from first sheet
        inputs = self.input()
        with next(inputs).open('r') as first_sheet:
            date_line = first_sheet.readlines()[7]
            date = date_line.split(',')[2]

        # get remaining data from second sheet
        with next(inputs).open('r') as second_sheet:
            df = pd.read_csv(second_sheet, skipfooter=1, engine='python')
            df.insert(2, 'date', pd.to_datetime(date, format='"%d.%m.%Y"'))

            count = df.pop('Gesamt')
            df.insert(3, 'count', count)

            df.columns = [el[0] for el in _columns]
            
            df['id'] = df['id'].apply(int)
            df['count'] = df['count'].apply(int)
            for i in range(24):
                df[f'"{i}"'] = df[f'"{i}"'].apply(self.safe_parse_int)

        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False, header=True)

    def safe_parse_int(self, val):
        return int(np.nan_to_num(val))
#!/usr/bin/env python3
import luigi
import pandas as pd

from csv_to_db import CsvToDb
from gomus_report import FetchGomusReport

_columns = [
        ('id', 'INT'),
        ('ticket', 'TEXT'),
        ('date', 'DATE'),
        ('count', 'INT'),
        [(str(i), 'INT') for i in range(24)]
    ]
_primary_key = ('id', 'date')

class DailyEntriesToDB(CsvToDb):
    table = 'gomus_daily_entry'
    columns = _columns
    primary_key = _primary_key

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return ExtractDailyEntryData(expected=False)

class ExpectedDailyEntriesToDB(CsvToDb):
    table = 'gomus_expected_daily_entry'
    columns = _columns
    primary_key = _primary_key

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return ExtractDailyEntryData(columns=[el[0] for el in self.columns], expected=True)

class ExtractDailyEntryData(luigi.Task):
    expected = luigi.parameter.BoolParameter(description="Whether to return actual or expected entries")

    def requires(self):
        return FetchGomusReport(report='entry', suffix='_1day', sheet_indices=[0, 1] if self.expected else [2, 3])

    def output(self):
        if self.expected:
            return luigi.LocalTarget('output/gomus/daily_entries.csv')
        return luigi.LocalTarget('output/gomus/expected_daily_entries.csv')

    def run(self):
        print(self.input())
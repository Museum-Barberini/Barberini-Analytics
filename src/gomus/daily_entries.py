"""Provides tasks for downloading the daily entries from Gomus into the DB."""

import datetime as dt

import luigi
import numpy as np
import pandas as pd
from luigi.format import UTF8

from _utils import CsvToDb, DataPreparationTask
from gomus._utils.fetch_report import FetchGomusReport


class DailyEntriesToDb(CsvToDb):
    """Store the fetched daily entry numbers into the database."""

    table = 'gomus_daily_entry'

    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    def requires(self):
        return ExtractDailyEntryData(
            expected=False,
            columns=[col[0] for col in self.columns],
            today=self.today)


class ExpectedDailyEntriesToDb(CsvToDb):
    """Store the expected daily entries into the database."""

    table = 'gomus_expected_daily_entry'

    today = luigi.parameter.DateParameter(default=dt.datetime.today())

    def requires(self):
        return ExtractDailyEntryData(
            expected=True,
            columns=[col[0] for col in self.columns],
            today=self.today)


class ExtractDailyEntryData(DataPreparationTask):
    """Extract daily entry data from the gomus web interface."""

    today = luigi.parameter.DateParameter(default=dt.datetime.today())
    expected = luigi.parameter.BoolParameter(
        description="Whether to return actual or expected entries")
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):

        for report in ['entries', 'entries_unique']:
            yield FetchGomusReport(
                report=report,
                suffix='_1day',
                sheet_indices=[0, 1] if not self.expected else [2, 3],
                today=self.today)

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/{"expected_" if self.expected else ""}'
            'daily_entries.csv', format=UTF8
        )

    def run(self):

        all_inputs = self.input()
        # get date from first sheet
        inputs = all_inputs[0]
        with next(inputs).open('r') as first_sheet:
            while True:
                try:
                    date_line = first_sheet.readline()
                    date = pd.to_datetime(
                        date_line.split(',')[2], format='"%d.%m.%Y"')
                    break
                except ValueError:
                    continue

        entries = {}
        # parse normal and unique entries to equivalent data frames
        for report_number in range(0, 2):
            # get remaining data from second sheet
            if report_number == 1:
                next(all_inputs[report_number])

            with next(all_inputs[report_number]).open('r') as second_sheet:
                df = pd.read_csv(second_sheet, skipfooter=1, engine='python')
                current_df = pd.DataFrame(columns=self.columns)

                for index, row in df.iterrows():
                    for i in range(24):
                        row_index = index * 24 + i
                        current_df.at[row_index, 'id'] = int(
                            np.nan_to_num(row['ID']))
                        current_df.at[row_index, 'ticket'] = row['Ticket']

                        time = dt.time(hour=i)
                        current_df.at[row_index, 'datetime'] = \
                            dt.datetime.combine(date, time)

                        # different hour formats for expected/actual entries
                        if self.expected:
                            count_index = str(i) + ':00'
                        else:
                            count_index = str(float(i))
                        current_df.at[row_index, 'count'] = \
                            self.safe_parse_int(row[count_index])

                entries[report_number] = current_df

        # combine the data frames
        entries[1]['unique_count'] = entries[1]['count']
        entries[1] = entries[1].drop(['count'], axis=1)
        entries[0] = entries[0].drop(['unique_count'], axis=1)

        combined_df = pd.merge(entries[0], entries[1])

        with self.output().open('w') as output_csv:
            combined_df.to_csv(output_csv, index=False, header=True)

    def safe_parse_int(self, val):
        return int(np.nan_to_num(val))

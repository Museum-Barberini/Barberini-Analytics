#!/usr/bin/env python3
import os
import time

import luigi
import requests
from luigi.format import UTF8

from .edit_report import EditGomusReport
from .fetch_report_helper import *


class FetchGomusReport(luigi.Task):
    report = luigi.parameter.Parameter(description="The report name (e.g. \'bookings\')")
    suffix = luigi.parameter.OptionalParameter(default='_7days', description="The report suffix (default: \'_7days\')")
    sheet_indices = luigi.parameter.ListParameter(default=[0], description="Page numbers of the Excel sheet")
    refresh_wait_time = luigi.parameter.IntParameter(default=60, description="How long to wait for the report to refresh")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.report_name = self.report + self.suffix
    
    def output(self):
        for index in self.sheet_indices:
            yield luigi.LocalTarget(f'output/gomus/{self.report_name}.{index}.csv', format=UTF8)

    def requires(self):
        if REPORT_IDS[f'{self.report_name}'] > 0: # report refreshable
            start_time, end_time = parse_timespan(self.suffix.replace('_', ''))
            yield EditGomusReport(report=REPORT_IDS[self.report_name], start_at=start_time, end_at=end_time)

    def run(self):
        sess_id = os.environ['GOMUS_SESS_ID']
        
        res_content = request_report(args=['-s', f'{sess_id}', '-t', f'{self.report_name}', '-l'])
        for index, target in enumerate(self.output()):
            with target.open('w') as target_csv:
                csv_from_excel(res_content, target_csv, self.sheet_indices[index])


class FetchEventReservations(luigi.Task):
    booking_id = luigi.parameter.IntParameter(description="The booking's index")
    status = luigi.parameter.IntParameter(description="ID of stats (0 = booked, 1 = cancelled) (default: 0)", default=0)

    def output(self):
        return luigi.LocalTarget(f'output/gomus/reservations/reservations_{self.booking_id}.{self.status}.csv', format=UTF8)

    def run(self):
        url = f'https://barberini.gomus.de/bookings/{self.booking_id}/seats.xlsx'
        res_content = requests.get(url, cookies=dict(_session_id=os.environ['GOMUS_SESS_ID'])).content
        with self.output().open('w') as target_csv:
            csv_from_excel(res_content, target_csv, self.status)

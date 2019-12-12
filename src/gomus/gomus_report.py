#!/usr/bin/env python3
import luigi
from luigi.format import UTF8
from fetch_gomus import request_report, csv_from_excel, report_ids
import os
import time

class FetchGomusReport(luigi.Task):
	report = luigi.parameter.Parameter(description="The report name (e.g. 'bookings')")
	suffix = luigi.parameter.OptionalParameter(default='_7days', description="The report suffix (default: '_7days')")
	
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.report_name = self.report + self.suffix
	
	def output(self):
		return luigi.LocalTarget(f'output/{self.report_name}.csv', format=UTF8)
	
	def run(self):
		sess_id = os.environ['GOMUS_SESS_ID']
		
		if report_ids[f'{self.report_name}'] > 0: # report refreshable
			request_report(args=['-s', f'{sess_id}', '-t', f'{self.report_name}', 'refresh'])
			print("Waiting 60 seconds for the report to refresh")
			time.sleep(60)
		
		res_content = request_report(args=['-s', f'{sess_id}', '-t', f'{self.report_name}', '-l'])
		with self.output().open('w') as target_csv:
			csv_from_excel(res_content, target_csv)

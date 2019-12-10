#!/usr/bin/env python3
import luigi
from luigi.format import UTF8
from fetch_gomus import request_report, csv_from_excel, report_ids
import os
import time

class FetchGomusReport(luigi.Task):
	report = luigi.parameter.Parameter()

	def output(self):
		return luigi.LocalTarget(f'output/{self.report}_7days.csv', format=UTF8)

	def run(self):
		sess_id = os.environ['GOMUS_SESS_ID']

		if report_ids[f'{self.report}_7days'] > 0: # report refreshable
			request_report(args=['-s', f'{sess_id}', '-t', f'{self.report}_7days', 'refresh'])
			print('Waiting 60 seconds for the report to refresh')
			time.sleep(60)

		res_content = request_report(args=['-s', f'{sess_id}', '-t', f'{self.report}_7days', '-l'])
		with self.output().open('w') as target_csv:
			csv_from_excel(res_content, target_csv)
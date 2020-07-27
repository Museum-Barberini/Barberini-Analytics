import datetime as dt

import luigi

from _utils import OUTPUT_DIR
from .log_report import SendLogReport


class Diagnostics(luigi.Task):

	def output(self):

		return luigi.LocalTarget(f'{OUTPUT_DIR}/diagnostics.txt')

	def run(self):

		tasks = 0

		if dt.date.today().isoweekday() == 7:  # sunday
			yield SendLogReport()
			tasks += 1

		# Pseudo output file
		with self.output().open('w') as txt:
			txt.write(f"Ran {tasks} tasks successfully.")

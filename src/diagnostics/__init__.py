"""Defines diagnostics tasks for monitoring the overall system stability."""

import datetime as dt

import luigi

from _utils import minimal_mode, output_dir
from .log_report import SendLogReport


class Diagnostics(luigi.Task):
    """Runs diagnostics tasks for monitoring the overall system stability."""

    def output(self):

        return luigi.LocalTarget(f'{output_dir()}/diagnostics.txt')

    def run(self):

        tasks = 0

        if dt.date.today().isoweekday() == 7 or minimal_mode:  # sunday
            yield SendLogReport()
            tasks += 1

        # Pseudo output file
        with self.output().open('w') as txt:
            txt.write(f"Ran {tasks} tasks successfully.")

import unittest
from unittest.mock import patch
import datetime as dt
import pandas as pd

from luigi.format import UTF8
from luigi.mock import MockTarget

from gomus._utils.fetch_report import FetchGomusReport
from gomus._utils.edit_report import EditGomusReport
from gomus._utils.fetch_report_helper import REPORT_IDS


class TestGomusEditReport(unittest.TestCase):
    @patch.object(FetchGomusReport, 'output')
    def test_edit_gomus_report_customers(self, output_mock):
        """
        This test edits the customer_7days report twice using different
        timespans and checks, if the report is updated by testing if the
        data is in the timespan that we set for the report.
        """
        for start_at in [dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 1)]:
            mock_target = MockTarget('customer_data_out', format=UTF8)
            output_mock.return_value = iter([mock_target])

            EditGomusReport(
                report=REPORT_IDS['customers_7days'],
                start_at=start_at,
                end_at=start_at + dt.timedelta(days=7)).run()
            FetchGomusReport(report='customers').run()

            with mock_target.open('r') as output:
                df = pd.read_csv(output)
                df.apply(
                    lambda x: self.check_date(x['Erstellt am'], start_at),
                    axis=1)

    def check_date(self, string, start_at):
        date = dt.datetime.strptime(string, '%d.%m.%Y')
        self.assertTrue((start_at <= date) and
                        (date <= start_at + dt.timedelta(days=7)),
                        "The customer_7days report isn't edited in the right "
                        "way, the dates don't match the given timespan")

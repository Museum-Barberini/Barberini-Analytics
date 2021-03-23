import datetime as dt
from unittest.mock import patch

from bs4 import BeautifulSoup

from db_test import DatabaseTestCase
from diagnostics.log_report import SendLogReport


class TestLogReport(DatabaseTestCase):
    """Test the diagnostics.log_report module."""

    @patch('diagnostics.log_report.send_error_email')
    def test_empty(self, mock_email):

        self.task = SendLogReport(today=dt.date(2015, 3, 14), days_back=0)

        self.run_task(self.task)

        mock_email.assert_called_once()
        message = mock_email.call_args_list[0][1]['message']
        self.assertTrue(BeautifulSoup(message, "html.parser").find())

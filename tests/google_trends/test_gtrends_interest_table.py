import datetime as dt

import pandas as pd

from google_trends.gtrends_interest_table import *
from task_test import DatabaseTaskTest


class TestGtrendsInterestTable(DatabaseTaskTest):
    def __init__(self, methodName):
        super().__init__(methodName)
        self.task = self.isolate(GTrendsInterestTable())

    def test(self):
        self.task = GTrendsInterestTable()
        self.task.run()
        csv = pd.read_csv("output/google-trends/interests.csv")
        self.assertFalse(csv.empty)
        self.assertEqual(2, csv.ndim)
        self.assertCountEqual(
            ['topicId', 'date', 'interestValue'], list(csv.columns))


class TestGtrendsInterestToDB(DatabaseTaskTest):
    def test_interestsToDB(self):
        GtrendsInterestToDB().run()

        result = self.db.request(
            "SELECT * FROM gtrends_interest WHERE topic_id LIKE 'TESTING_%'")

        self.assertListEqual(
            ['topic_id', 'date', 'interest_value'], self.db.column_names)
        self.assertListEqual([
            ('TESTING_foo', dt.date(year=2001, month=10, day=12), 9),
            ('TESTING_bar', dt.date(year=2017, month=1, day=20), 42)],
            result
        )

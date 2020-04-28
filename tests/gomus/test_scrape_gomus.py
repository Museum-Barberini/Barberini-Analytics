import sys
from unittest.mock import patch

import mmh3
import pandas as pd
from luigi.format import UTF8
from luigi.mock import MockTarget

from gomus._utils.extract_bookings import ExtractGomusBookings
from gomus._utils.fetch_htmls import FetchBookingsHTML, FetchGomusHTML
from gomus._utils.scrape_gomus import (EnhanceBookingsWithScraper,
                                       ScrapeGomusOrderContains)
from task_test import DatabaseTaskTest
from tests.gomus.test_gomus_transformations import BOOKING_COLUMNS


class TestEnhanceBookingsWithScraper(DatabaseTaskTest):
    '''
    This test gets a set of booking-IDs (in test_data/scrape_bookings_data.csv)
    and downloads their actual HTML-files.
    It then compares the expected hash (also in the file above)
    with the hash of what our scraper got.

    To easily add test cases, use create_test_data_for_bookings.py.
    '''

    hash_seed = 666

    @patch.object(EnhanceBookingsWithScraper, 'fetch_updated_mail')
    @patch.object(EnhanceBookingsWithScraper, 'ensure_foreign_keys')
    @patch.object(ExtractGomusBookings, 'output')
    @patch.object(FetchBookingsHTML, 'output')
    @patch.object(EnhanceBookingsWithScraper, 'output')
    def test_scrape_bookings(self,
                             output_mock,
                             all_htmls_mock,
                             input_mock,
                             foreign_key_mock,
                             new_mail_mock):

        test_data = pd.read_csv(
            'tests/test_data/gomus/scrape_bookings_data.csv')

        # We generate random stuff for extracted_bookings,
        # because the scraper doesn't need it for any calculations.
        # This resembles existing booking-data
        # that should not get manipulated by the scraper.
        test_data.insert(1, 'some_other_value', test_data.index)

        extracted_bookings = test_data.filter(
            ['booking_id',
             'some_other_value'])

        input_target = MockTarget('extracted_bookings_out', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as input_file:
            extracted_bookings.to_csv(input_file, index=False)

        # download htmls (similar to what FetchBookingsHTML does)
        html_file_names = []
        for i, row in extracted_bookings.iterrows():
            booking_id = row['booking_id']
            new_html_task = FetchGomusHTML(
                url=f"https://barberini.gomus.de/admin/bookings/{booking_id}")
            new_html_task.run()
            html_file_names.append(new_html_task.output().path)

        all_htmls_target = MockTarget('bookings_htmls_out', format=UTF8)
        all_htmls_mock.return_value = all_htmls_target
        with all_htmls_target.open('w') as all_htmls_file:
            all_htmls_file.write('\n'.join(html_file_names))

        output_target = MockTarget('enhanced_bookings_out', format=UTF8)
        output_mock.return_value = output_target

        # Also test that fetch_updated_mail would be called for non-empty
        # invalid_values (actual functionality tested elsewhere)
        invalid_values = pd.DataFrame([0], columns=['booking_id'])
        foreign_key_mock.side_effect = lambda x: (x, invalid_values)

        new_mail_mock.return_value = iter([
            FetchGomusHTML(url='test1'),
            FetchGomusHTML(url='test2')])

        # -- execute code under test --
        self.task = EnhanceBookingsWithScraper(columns=BOOKING_COLUMNS)
        run = self.task.run()
        for yielded_task in run:
            self.assertIsInstance(yielded_task, FetchGomusHTML)

        # -- inspect results --
        expected_output = test_data.filter(
            ['booking_id',
             'some_other_value',
             'expected_hash'])

        with output_target.open('r') as output_file:
            actual_output = pd.read_csv(output_file)

        self.assertEqual(len(expected_output.index), len(actual_output.index))

        for i in range(len(actual_output)):
            expected_row = expected_output.iloc[i]
            actual_row = actual_output.iloc[i]

            # test if order stayed the same
            self.assertEqual(
                expected_row['booking_id'],
                actual_row['booking_id'])

            # test if existing data got modified
            self.assertEqual(
                expected_row['some_other_value'],
                actual_row['some_other_value'])

            # test if scraped data is correct
            hash_str = ','.join([
                str(actual_row['customer_id']),
                str(actual_row['order_date']),
                str(actual_row['language'])])
            actual_hash = mmh3.hash(hash_str, seed=self.hash_seed)
            self.assertEqual(
                actual_hash,
                expected_row['expected_hash'],
                msg=f"Scraper got wrong values:\n\
{str(actual_row) if sys.stdin.isatty() else 'REDACTED ON NON-TTY'}")

    def test_new_mail_fetching(self):

        # Prepare DB values
        gomus_id = 270287
        booking_id = 14212
        fake_customer_id = 121212
        real_customer_id = 1726152420

        try:
            # Set up tables. This is unnecessary when the test DB's
            # schema automatically equals that of the actual DB
            self.db_connector.execute(
                '''CREATE TABLE gomus_customer (
                    customer_id INTEGER
                )''',
                '''CREATE TABLE gomus_to_customer_mapping (
                    gomus_id INTEGER,
                    customer_id INTEGER
                )''',
                '''ALTER TABLE gomus_customer
                    ADD CONSTRAINT customer_id_primkey
                    PRIMARY KEY (customer_id)
                ''',
                '''ALTER TABLE gomus_to_customer_mapping
                    ADD CONSTRAINT customer_id_fkey
                    FOREIGN KEY (customer_id)
                        REFERENCES gomus_customer (customer_id)
                    ON UPDATE CASCADE
                ''')

            # Insert test values
            self.db_connector.execute(
                f'INSERT INTO gomus_customer VALUES ({fake_customer_id})',
                f'''INSERT INTO gomus_to_customer_mapping VALUES (
                        {gomus_id}, {fake_customer_id})
                    ''')

            # Run fetch_updated_mail
            self.task = EnhanceBookingsWithScraper(columns=BOOKING_COLUMNS)
            fetch_mail = self.task().fetch_updated_mail(booking_id)
            for html_task in fetch_mail:
                html_task.run()

            # Check DB values
            new_id = self.db_connector.query(
                f'''
                    SELECT customer_id
                    FROM gomus_to_customer_mapping
                    WHERE gomus_id = {gomus_id}
                ''',
                only_first=True)
            self.assertEqual(new_id[0], real_customer_id)

        finally:
            # Clear DB again
            self.db_connector.execute(
                'DROP TABLE gomus_to_customer_mapping',
                'DROP TABLE gomus_customer')


class TestScrapeOrderContains(DatabaseTaskTest):

    hash_seed = 666

    @patch.object(ScrapeGomusOrderContains, 'input')
    @patch.object(ScrapeGomusOrderContains, 'output')
    def test_scrape_order_contains(self, output_mock, input_mock):

        test_data = pd.read_csv(
            'tests/test_data/gomus/scrape_order_contains_data.csv')

        html_file_names = []
        all_order_ids = test_data['order_id'].drop_duplicates()
        for order_id in all_order_ids:
            new_html_task = FetchGomusHTML(
                url=f"https://barberini.gomus.de/admin/orders/{order_id}")
            new_html_task.run()
            html_file_names.append(new_html_task.output().path)

        input_target = MockTarget('orders_htmls_out', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as input_file:
            input_file.write('\n'.join(html_file_names))

        output_target = MockTarget('scraped_order_contains_out', format=UTF8)
        output_mock.return_value = output_target

        # -- execute code under test --
        ScrapeGomusOrderContains().run()

        # -- inspect results --
        with output_target.open('r') as output_file:
            actual_output = pd.read_csv(output_file)

        self.assertEqual(len(test_data.index), len(actual_output.index))

        for i in range(len(actual_output)):
            expected_row = test_data.iloc[i]
            actual_row = actual_output.iloc[i]

            # test if order stayed the same
            self.assertEqual(
                expected_row['order_id'],
                actual_row['order_id'])

            # test if scraped data is correct
            hash_string = ','.join([
                str(actual_row['article_id']),
                str(actual_row['article_type']),
                str(actual_row['ticket']),
                str(actual_row['date']),
                str(actual_row['quantity']),
                str(actual_row['price'])])
            actual_hash = mmh3.hash(hash_string, seed=self.hash_seed)
            self.assertEqual(
                actual_hash,
                expected_row['expected_hash'],
                msg=f"Scraper got wrong values:\n\
{str(actual_row) if sys.stdin.isatty() else 'REDACTED ON NON-TTY'}")

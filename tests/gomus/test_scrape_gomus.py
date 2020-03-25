import unittest
from unittest.mock import patch
from luigi.mock import MockTarget
from luigi.format import UTF8

import pandas as pd

from gomus._utils.scrape_gomus import\
    EnhanceBookingsWithScraper,\
    FetchBookingsHTML
from gomus._utils.extract_bookings import ExtractGomusBookings


class TestEnhanceBookingsWithScraper(unittest.TestCase):
    @patch.object(ExtractGomusBookings, 'output')
    @patch.object(FetchBookingsHTML, 'output')
    @patch.object(EnhanceBookingsWithScraper, 'output')
    def test_scrape_bookings(self,
                             output_mock,
                             all_htmls_mock,
                             input_mock):
        # This also runs FetchBookingsHTML with fake data

        test_data = pd.read_csv(
            'tests/test_data/gomus/scrape_bookings_data.csv')

        # we generate random stuff for extracted_bookings,
        # because the scraper doesn't need it for calculations
        extracted_bookings = test_data.filter(['booking_id', 'testcase_descr'])
        extracted_bookings['some_other_value'] = extracted_bookings.index

        input_target = MockTarget('extracted_bookings_out', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open('w') as input_file:
            extracted_bookings.to_csv(input_file)

        all_htmls_target = MockTarget('bookings_htmls_out', format=UTF8)
        all_htmls_mock.return_value = all_htmls_target

        output_target = MockTarget('enhanced_bookings_out', format=UTF8)
        output_mock.return_value = output_target
        print(all_htmls_target.exists())
        FetchBookingsHTML(
            timespan='_nextYear',
            base_url="https://barberini.gomus.de/admin/bookings/").run()
        EnhanceBookingsWithScraper(timespan='_nextYear').run()

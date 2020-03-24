import unittest
from unittest.mock import patch
from luigi.mock import MockTarget
from luigi.format import UTF8

import pandas as pd

from gomus._utils.scrape_gomus import EnhanceBookingsWithScraper
from gomus._utils.extract_bookings import ExtractGomusBookings


class TestEnhanceBookingsWithScraper(unittest.TestCase):
    @patch.object(ExtractGomusBookings, 'output')
    @patch.object(EnhanceBookingsWithScraper, 'output')
    def test_scrape_bookings(self,
                             output_mock,
                             input_mock):
        # This actually also runs fetchBookingsHTML with fake data

        test_data = pd.read_csv(
            'tests/test_data/gomus/scrape_bookings_data.csv')

        # we generate random stuff for extracted_bookings,
        # because the scraper doesn't need it for calculations
        extracted_bookings = test_data.filter(['booking_id', 'testcase_descr'])
        extracted_bookings['some_other_value'] = extracted_bookings.index

        input_target = MockTarget('extracted_bookings_out', format=UTF8)
        input_mock.return_value = input_target
        with input_target.open(mode='w') as input_file:
            extracted_bookings.to_csv(input_file)

        output_target = MockTarget('enhanced_bookings_out', format=UTF8)
        output_mock.return_value = output_target

        EnhanceBookingsWithScraper().run()

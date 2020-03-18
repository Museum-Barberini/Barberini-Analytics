import unittest
from unittest.mock import patch, PropertyMock

from luigi.format import UTF8
from luigi.mock import MockTarget
from luigi.parameter import UnknownParameterException

from gomus.customers import ExtractCustomerData
from gomus.events import ExtractEventData
from gomus.orders import ExtractOrderData
from gomus._utils.extract_bookings import ExtractGomusBookings
from gomus.daily_entries import ExtractDailyEntryData
from task_test import DatabaseHelper


class GomusTransformationTest(unittest.TestCase):
    def __init__(self, columns, task, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.task = task

        self.test_data_path = 'tests/test_data/gomus/'

        # TODO: Set up proper MockFileSystem isolation between tests
        # (apparently, this is just kept constantly otherwise)

    def _write_file_to_target(self, target, filename):
        filename = self.test_data_path + filename

        with target.open('w') as input_data:
            with open(filename, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

    def _prepare_input_target(self, input_mock, infile):
        input_target = MockTarget('data_in', format=UTF8)

        # FetchGomusReport returns iterable, to simulate this for most tasks:
        input_mock.return_value = iter([input_target])

        self._write_file_to_target(input_target, infile)

    def _prepare_output_target(self, output_mock):
        output_target = MockTarget('data_out', format=UTF8)
        output_mock.return_value = output_target
        return output_target

    def prepare_mock_targets(self, input_mock, output_mock, infile):
        # Overwrite input and output of target task with MockTargets
        self._prepare_input_target(input_mock, infile)
        output_target = self._prepare_output_target(output_mock)

        return output_target

    def execute_task(self, **kwargs):
        try:
            self.task(columns=self.columns, **kwargs).run()
        except UnknownParameterException:
            self.task(**kwargs).run()

    def check_result(self, output_target, outfile):
        outfile = self.test_data_path + outfile

        with output_target.open('r') as output_data:
            with open(outfile, 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())


class TestCustomerTransformation(GomusTransformationTest):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'gomus_id',
            'customer_id',
            'postal_code',
            'newsletter',
            'gender',
            'category',
            'language',
            'country',
            'type',
            'register_date',
            'annual_ticket'],
            ExtractCustomerData,
            *args, **kwargs)

        self.test_data_path += 'customers/'

    @patch.object(ExtractCustomerData, 'output')
    @patch.object(ExtractCustomerData, 'input')
    def test_customer_transformation(self, input_mock, output_mock):
        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'customers_in.csv')

        self.execute_task()

        self.check_result(output_target, 'customers_out.csv')

    @patch.object(ExtractCustomerData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        self._prepare_input_target(input_mock, 'customers_invalid_date.csv')

        # 30.21.2005 should not be a valid date
        self.assertRaises(ValueError, self.execute_task)


class TestOrderTransformation(GomusTransformationTest):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'order_id',
            'order_date',
            'customer_id',
            'valid',
            'paid',
            'origin'],
            ExtractOrderData,
            *args, **kwargs)

        self.test_data_path += 'orders/'
        self.db_helper = DatabaseHelper()

    # Provide mock customer IDs to be found by querying
    def setUp(self):
        self.db_helper.setUp()
        self.db_helper.commit(
            ('CREATE TABLE gomus_customer '
             '(gomus_id INTEGER, customer_id INTEGER)'),
            'INSERT INTO gomus_customer VALUES (117899, 100)'
        )

    def tearDown(self):
        self.db_helper.commit(
            'DROP TABLE gomus_customer'
        )
        self.db_helper.tearDown()

    @patch.object(ExtractOrderData, 'database', new_callable=PropertyMock)
    @patch.object(ExtractOrderData, 'output')
    @patch.object(ExtractOrderData, 'input')
    def test_order_transformation(self,
                                  input_mock,
                                  output_mock,
                                  database_mock):

        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'orders_in.csv')

        database_mock.return_value = 'barberini_test'

        self.execute_task()

        self.check_result(output_target, 'orders_out.csv')

    @patch.object(ExtractOrderData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        self._prepare_input_target(input_mock, 'orders_invalid_date.csv')

        # 10698846.0 should be out of range
        self.assertRaises(OverflowError, self.execute_task)


# This tests only ExtractGomusBookings, the scraper should be tested elsewhere
class TestBookingTransformation(GomusTransformationTest):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'booking_id',
            'customer_id',
            'category',
            'participants',
            'guide_id',
            'duration',
            'exhibition',
            'title',
            'status',
            'start_datetime'],
            ExtractGomusBookings,
            *args, **kwargs)

        self.test_data_path += 'bookings/'

    @patch.object(ExtractGomusBookings, 'output')
    @patch.object(ExtractGomusBookings, 'input')
    def test_booking_transformation(self, input_mock, output_mock):
        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'bookings_in.csv')

        self.execute_task()

        self.check_result(
            output_target,
            'bookings_out.csv')

    @patch.object(ExtractGomusBookings, 'output')
    @patch.object(ExtractGomusBookings, 'input')
    def test_empty_bookings(self, input_mock, output_mock):
        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'bookings_empty_in.csv')

        self.execute_task()

        self.check_result(
            output_target,
            'bookings_empty_out.csv')


class TestDailyEntryTransformation(GomusTransformationTest):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'id',
            'ticket',
            'datetime',
            'count'],
            ExtractDailyEntryData,
            *args, **kwargs)

        self.test_data_path += 'daily_entries/'

    # Don't prepare targets like usual because two inputs are expected
    def prepare_mock_targets(self,
                             input_mock,
                             output_mock,
                             infile_1,
                             infile_2):
        input_target_1 = MockTarget('data_in_1', format=UTF8)
        input_target_2 = MockTarget('data_in_2', format=UTF8)
        input_mock.return_value = iter([input_target_1, input_target_2])
        output_target = self._prepare_output_target(output_mock)

        self._write_file_to_target(input_target_1, infile_1)
        self._write_file_to_target(input_target_2, infile_2)

        return output_target

    @patch.object(ExtractDailyEntryData, 'output')
    @patch.object(ExtractDailyEntryData, 'input')
    def test_actual_daily_entry_transformation(
            self, input_mock, output_mock):

        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'daily_entry_actual_in_1.csv',
            'daily_entry_actual_in_2.csv')

        self.execute_task(expected=False)

        self.check_result(
            output_target,
            'daily_entry_actual_out.csv')

    @patch.object(ExtractDailyEntryData, 'output')
    @patch.object(ExtractDailyEntryData, 'input')
    def test_expected_daily_entry_transformation(
            self, input_mock, output_mock):

        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'daily_entry_expected_in_1.csv',
            'daily_entry_expected_in_2.csv')

        self.execute_task(expected=True)

        self.check_result(
            output_target,
            'daily_entry_expected_out.csv')


class TestEventTransformation(GomusTransformationTest):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'event_id',
            'customer_id',
            'booking_id',
            'reservation_count',
            'order_date',
            'status',
            'category'],
            ExtractEventData,
            *args, **kwargs)

        self.test_data_path += 'events/'

        self.categories = [
            'Oeffentliche Fuehrung',
            'Event',
            'Gespraech',
            'Kinder-Workshop',
            'Konzert',
            'Lesung',
            'Vortrag']

    @patch.object(ExtractEventData, 'output')
    @patch.object(ExtractEventData, 'input')
    def test_events_transformation(self, input_mock, output_mock):
        def generate_input_targets():
            for category in self.categories:
                target = MockTarget(category, format=UTF8)
                self._write_file_to_target(target, category + '_in.csv')
                yield target

        input_mock.return_value = generate_input_targets()

        output_target = self._prepare_output_target(output_mock)

        self.execute_task()

        self.check_result(
            output_target,
            'events_out.csv')

    # TODO: Properly test 'EnsureBookingsIsRun'

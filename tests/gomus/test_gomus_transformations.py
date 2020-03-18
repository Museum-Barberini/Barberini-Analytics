import unittest
from unittest.mock import patch

from luigi.format import UTF8
from luigi.mock import MockTarget

from gomus.customers import ExtractCustomerData
from gomus.orders import ExtractOrderData


class TestGomusTransformation(unittest.TestCase):
    def __init__(self, columns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.test_data_path = 'tests/test_data/gomus/'

    def _prepare_input_target(self, input_mock, infile):
        infile = self.test_data_path + infile

        input_target = MockTarget('data_in', format=UTF8)

        # FetchGomusReport returns iterable, to simulate this for most tasks:
        input_mock.return_value = iter([input_target])

        # Write test data to input mock
        with input_target.open('w') as input_data:
            with open(infile, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

    def _prepare_output_target(self, output_mock):
        output_target = MockTarget('data_out', format=UTF8)
        output_mock.return_value = output_target
        return output_target

    def prepare_mock_targets(self, input_mock, output_mock, infile):
        # Overwrite input and output of target task with MockTargets
        self._prepare_input_target(input_mock, infile)
        output_target = self._prepare_output_target(output_mock)

        return output_target

    def check_result(self, output_target, outfile):
        outfile = self.test_data_path + outfile

        with output_target.open('r') as output_data:
            with open(outfile, 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())


class TestCustomerTransformation(TestGomusTransformation):
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
            *args, **kwargs)

    @patch.object(ExtractCustomerData, 'output')
    @patch.object(ExtractCustomerData, 'input')
    def test_customer_transformation(self, input_mock, output_mock):
        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'customers_in.csv')

        # Execute task
        ExtractCustomerData(self.columns).run()

        self.check_result(output_target, 'customers_out.csv')

    @patch.object(ExtractCustomerData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        self._prepare_input_target(input_mock, 'customers_invalid_date.csv')

        # 30.21.2005 should not be a valid date
        self.assertRaises(ValueError, ExtractCustomerData(self.columns).run)


class TestOrderTransformation(TestGomusTransformation):
    def __init__(self, *args, **kwargs):
        super().__init__([
            'order_id',
            'order_date',
            'customer_id',
            'valid',
            'paid',
            'origin'],
            *args, **kwargs)

    @patch.object(ExtractOrderData, 'query_customer_id')
    @patch.object(ExtractOrderData, 'output')
    @patch.object(ExtractOrderData, 'input')
    def test_order_transformation(self, input_mock, output_mock, cust_id_mock):
        output_target = self.prepare_mock_targets(
            input_mock,
            output_mock,
            'orders_in.csv')

        cust_id_mock.return_value = 0

        ExtractOrderData(self.columns).run()

        self.check_result(output_target, 'orders_out.csv')

    @patch.object(ExtractOrderData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        self._prepare_input_target(input_mock, 'orders_invalid_date.csv')

        # 10698846.0 should be out of range
        self.assertRaises(OverflowError, ExtractOrderData(self.columns).run)


# This tests only ExtractGomusBookings, the scraper should be tested elsewhere
class TestBookingTransformation(TestGomusTransformation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

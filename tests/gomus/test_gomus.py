#!/usr/bin/env python3
import os
import requests
import unittest

from luigi.format import UTF8
from luigi.mock import MockTarget
from unittest.mock import MagicMock
from unittest.mock import patch

from customers_to_db import ExtractCustomerData
from orders_to_db import ExtractOrderData

# Does not test whether data is put into DB correctly because csv_to_db is already tested

class TestGomusConnection(unittest.TestCase):
    def test_session_id_is_valid(self):
        # tests if GOMUS_SESS_ID env variable contains a valid session id
        response = requests.get('https://barberini.gomus.de/', cookies=dict(
            _session_id=os.environ['GOMUS_SESS_ID']),
            allow_redirects=False)
        self.assertEqual(response.status_code, 200)


class TestGomusCustomerTransformations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ['gomus_id', 'customer_id', 'postal_code', 'newsletter', 'gender', 'category', 'language', 'country', 'type', 'register_date', 'annual_ticket']
    
    @patch.object(ExtractCustomerData, 'output')
    @patch.object(ExtractCustomerData, 'input')
    def test_customers_transformation(self, input_mock, output_mock):
        # Overwrite input and output of target task with MockTargets
        input_target = MockTarget('customer_data_in', format=UTF8)
        output_target = MockTarget('customer_data_out', format=UTF8)
        input_mock.return_value = input_target
        output_mock.return_value = output_target
        
        # Write test data to input mock
        with input_target.open('w') as input_data:
            with open('tests/test_data/gomus_customers_in.csv', 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())
        
        # Execute task
        ExtractCustomerData(self.columns).run()
        
        # Check result in output mock
        with output_target.open('r') as output_data:
            with open('tests/test_data/gomus_customers_out.csv', 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())
    
    @patch.object(ExtractCustomerData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        input_target = MockTarget('customer_data_in', format=UTF8)
        input_mock.return_value = input_target
        
        with input_target.open('w') as input_data:
            with open('tests/test_data/gomus_customers_invalid_date.csv', 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())
        
        # 30.21.2005 should not be a valid date
        self.assertRaises(ValueError, ExtractCustomerData(self.columns).run)

class TestGomusOrdersTransformations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = ['order_id', 'order_date', 'customer_id', 'valid', 'paid', 'origin']
    
    @patch.object(ExtractOrderData, 'query_customer_id')
    @patch.object(ExtractOrderData, 'output')
    @patch.object(ExtractOrderData, 'input')
    def test_orders_transformation(self, input_mock, output_mock, cust_id_mock):
        # Overwrite input and output of target task with MockTargets
        input_target = MockTarget('order_data_in', format=UTF8)
        output_target = MockTarget('order_data_out', format=UTF8)
        input_mock.return_value = input_target
        output_mock.return_value = output_target
        cust_id_mock.return_value = 0
        
        # Write test data to input mock
        with input_target.open('w') as input_data:
            with open('tests/test_data/gomus_orders_in.csv', 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())
        
        # Execute task
        ExtractOrderData(self.columns).run()
        
        # Check result in output mock
        with output_target.open('r') as output_data:
            with open('tests/test_data/gomus_orders_out.csv', 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())
    
    @patch.object(ExtractOrderData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        input_target = MockTarget('customer_data_in', format=UTF8)
        input_mock.return_value = input_target
        
        with input_target.open('w') as input_data:
            with open('tests/test_data/gomus_orders_invalid_date.csv', 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())
        
        # 10698846.0 should be out of range
        self.assertRaises(OverflowError, ExtractOrderData(self.columns).run)

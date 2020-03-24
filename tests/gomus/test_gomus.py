import os
import unittest
from unittest.mock import patch
import datetime as dt

import requests
from luigi.format import UTF8
from luigi.mock import MockTarget

from gomus.customers import ExtractCustomerData
from gomus.orders import ExtractOrderData
from gomus._utils.fetch_report import FetchGomusReport


class TestGomusConnection(unittest.TestCase):
    """
    Does not test whether data is put into DB correctly
    because CsvToDb is tested separately.
    """

    def test_session_id_is_valid(self):
        # test if GOMUS_SESS_ID env variable contains a valid session id
        response = requests.get(
            'https://barberini.gomus.de/',
            cookies={'_session_id': os.environ['GOMUS_SESS_ID']},
            allow_redirects=False)
        self.assertEqual(response.status_code, 200)


class TestGomusCustomerTransformations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = [
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
            'annual_ticket']

    @patch.object(ExtractCustomerData, 'output')
    @patch.object(ExtractCustomerData, 'input')
    def test_customers_transformation(self, input_mock, output_mock):
        # Overwrite input and output of target task with MockTargets
        input_target = MockTarget('customer_data_in', format=UTF8)
        output_target = MockTarget('customer_data_out', format=UTF8)
        input_mock.return_value = iter([input_target])
        output_mock.return_value = output_target

        # Write test data to input mock
        with input_target.open('w') as input_data:
            filename = 'tests/test_data/gomus/customers_in.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

        # Execute task
        ExtractCustomerData(self.columns).run()

        # Check result in output mock
        with output_target.open('r') as output_data:
            filename = 'tests/test_data/gomus/customers_out.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())

    @patch.object(ExtractCustomerData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        input_target = MockTarget('customer_data_in', format=UTF8)
        input_mock.return_value = iter([input_target])

        with input_target.open('w') as input_data:
            filename = 'tests/test_data/gomus/customers_invalid_date.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

        # 30.21.2005 should not be a valid date
        self.assertRaises(ValueError, ExtractCustomerData(self.columns).run)


class TestGomusOrdersTransformations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = [
            'order_id',
            'order_date',
            'customer_id',
            'valid',
            'paid',
            'origin']

    @patch.object(ExtractOrderData, 'query_customer_id')
    @patch.object(ExtractOrderData, 'output')
    @patch.object(ExtractOrderData, 'input')
    def test_orders_transformation(
            self, input_mock, output_mock, cust_id_mock):
        # Overwrite input and output of target task with MockTargets
        input_target = MockTarget('order_data_in', format=UTF8)
        output_target = MockTarget('order_data_out', format=UTF8)
        input_mock.return_value = iter([input_target])
        output_mock.return_value = output_target
        cust_id_mock.return_value = 0

        # Write test data to input mock
        with input_target.open('w') as input_data:
            filename = 'tests/test_data/gomus/orders_in.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

        # Execute task
        ExtractOrderData(self.columns).run()

        # Check result in output mock
        with output_target.open('r') as output_data:
            filename = 'tests/test_data/gomus/orders_out.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_out:
                self.assertEqual(output_data.read(), test_data_out.read())

    @patch.object(ExtractOrderData, 'input')
    def test_invalid_date_raises_exception(self, input_mock):
        input_target = MockTarget('customer_data_in', format=UTF8)
        input_mock.return_value = iter([input_target])

        with input_target.open('w') as input_data:
            filename = 'tests/test_data/gomus/orders_invalid_date.csv'
            with open(filename, 'r', encoding='utf-8') as test_data_in:
                input_data.write(test_data_in.read())

        # 10698846.0 should be out of range
        self.assertRaises(OverflowError, ExtractOrderData(self.columns).run)


class TestReportFormats(unittest.TestCase):
    @patch.object(FetchGomusReport, 'output')
    def test_gomus_formats(self, output_mock):
        output_target = MockTarget('report_out', format=UTF8)

        today = dt.datetime.now().strftime("%d.%m.%Y")
        yesterday = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%d.%m.%Y")

        bookings_format = '"Buchung","Datum","Uhrzeit von","Uhrzeit bis","Museum",'\
            '"Ausstellung","Raum","Treffpunkt","Angebotskategorie","Titel",'\
            '"Teilnehmerzahl","Guide","Kunde","E-Mail","Institution","Notiz",'\
            '"Führungsentgelt / Summe Endkundenpreis ohne Storno","Honorar",'\
            '"Zahlungsart","Kommentar","Status"\n'
        orders_format = '"Bestellnummer","Erstellt","Kundennummer","Kunde",'\
            '"Bestellwert","Gutscheinwert","Gesamtbetrag","ist gültig?",'\
            '"Bezahlstatus","Bezahlmethode","ist storniert?","Herkunft",'\
            '"Kostenstellen","In Rechnung gestellt am","Rechnungen",'\
            '"Korrekturrechnungen","Rating"\n'
        customers_format = '"Nummer","Anrede","Vorname","Name","E-Mail",'\
            '"Telefon","Mobil","Fax","Sprache","Kategorie","Straße","PLZ",'\
            '"Stadt","Land","Typ","Erstellt am","Newsletter","Jahreskarte"\n'
        entries_0and2_sheet_format = f'"ID","Ticket","{yesterday}",'\
            f'"{today}","Gesamt",""\n'
        entries_1_sheet_format = '"ID","Ticket",0.0,1.0,2.0,3.0,4.0,5.0,'\
            '6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,'\
            '18.0,19.0,20.0,21.0,22.0,23.0,"Gesamt"\n'
        entries_3_sheet_format ='"ID","Ticket","0:00","1:00","2:00","3:00",'\
            '"4:00","5:00","6:00","7:00","8:00","9:00","10:00","11:00",'\
            '"12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00",'\
            '"20:00","21:00","22:00","23:00","Gesamt"\n'

        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'bookings',
                                                   bookings_format))
        #self.assertTrue(self.check_expected_format(output_mock,
        #                                           output_target,
        #                                           'orders',
        #                                           orders_format,
        #                                           '_1day'))
        #self.assertTrue(self.check_expected_format(output_mock,
        #                                           output_target,
        #                                           'orders',
        #                                           output_target,
        #                                           orders_format))
        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'customers',
                                                   customers_format))
        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'entries',
                                                   entries_0and2_sheet_format,
                                                   '_1day'))
        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'entries',
                                                   entries_1_sheet_format,
                                                   '_1day',
                                                   [1]))
        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'entries',
                                                   entries_0and2_sheet_format,
                                                   '_1day',
                                                   [2]))
        self.assertTrue(self.check_expected_format(output_mock,
                                                   output_target,
                                                   'entries',
                                                   entries_3_sheet_format,
                                                   '_1day',
                                                   [3]))

    def check_expected_format(self, output_mock, output_target, report, expected_format, suffix='_7days', sheet=[0]):
        output_mock.return_value = iter([output_target])
        FetchGomusReport(report=report, suffix=suffix, sheet_indices=sheet).run()
        with output_target.open('r') as output_file:
            return expected_format in output_file.read()
                 
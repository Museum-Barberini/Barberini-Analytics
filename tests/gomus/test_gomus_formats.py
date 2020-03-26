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


class TestReportFormats(unittest.TestCase):
    @patch.object(FetchGomusReport, 'output')
    def test_gomus_formats(self, output_mock):
        output_target = MockTarget('report_out', format=UTF8)

        end_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%d.%m.%Y")
        start_date = (dt.datetime.now() - dt.timedelta(days=2)).strftime("%d.%m.%Y")

        bookings_format = [
            ('"Buchung"', 'FLOAT'),
            ('"Datum"', 'DATE'),
            ('"Uhrzeit von"', 'TIME'),
            ('"Uhrzeit bis"', 'TIME'),
            ('"Museum"', 'STRING'),
            ('"Ausstellung"', 'STRING'),
            ('"Raum"', 'STRING'),
            ('"Treffpunkt"', 'STRING'),
            ('"Angebotskategorie"', 'STRING'),
            ('"Titel"', 'STRING'),
            ('"Teilnehmerzahl"', 'FLOAT'),
            ('"Guide"', 'STRING'),
            ('"Kunde"', 'STRING'),
            ('"E-Mail"', 'STRING'),
            ('"Institution"', 'STRING'),
            ('"Notiz"', 'STRING'),
            ('"Führungsentgelt / Summe Endkundenpreis ohne Storno"', 'FLOAT'),
            ('"Honorar"', 'FLOAT'),
            ('"Zahlungsart"', 'STRING'),
            ('"Kommentar"', 'STRING'),
            ('"Status"\n', 'STRING')
        ]
        orders_format = [ #doto: add type
            ('"Bestellnummer"', 'FLOAT'),
            ('"Erstellt"', 'DATE'),
            ('"Kundennummer"', 'FLOAT'),
            ('"Kunde"', 'STRING'),
            ('"Bestellwert"', 'STRING'),
            ('"Gutscheinwert"', 'STRING'),
            ('"Gesamtbetrag"', 'STRING'),
            ('"ist gültig?"', 'STRING'),
            ('"Bezahlstatus"', 'STRING'),
            ('"Bezahlmethode"', 'STRING'),
            ('"ist storniert?"', 'STRING'),
            ('"Herkunft"', 'STRING'),
            ('"Kostenstellen"', 'STRING'),
            ('"In Rechnung gestellt am"', 'STRING'),
            ('"Rechnungen"', 'STRING'),
            ('"Korrekturrechnungen"', 'STRING'),
            ('"Rating"\n', 'STRING')
        ]
        customers_format = [
            ('"Nummer"', 'FLOAT'),
            ('"Anrede"', 'DATE'),
            ('"Vorname"', 'TIME'),
            ('"Name"', 'TIME'),
            ('"E-Mail"', 'STRING'),
            ('"Telefon"', 'STRING'),
            ('"Mobil"', 'STRING'),
            ('"Fax"', 'STRING'),
            ('"Sprache"', 'STRING'),
            ('"Kategorie"', 'STRING'),
            ('"Straße"', 'STRING'),
            ('"PLZ"', 'STRING'),
            ('"Stadt"', 'STRING'),
            ('"Land"', 'STRING'),
            ('"Typ"', 'STRING'),
            ('"Erstellt am"', 'STRING'),
            ('"Newsletter"', 'STRING'),
            ('"Jahreskarte"\n', 'STRING')
        ]
        entries_0and2_sheet_format = [
            ('"ID"', 'FLOAT'),
            ('"Ticket"', 'STRING'),
            (f'"{start_date}"', 'STRING'),
            (f'"{end_date}"', 'STRING'),
            ('"Gesamt"', 'STRING'),
            ('""\n', 'STRING')
        ]
        entries_1_sheet_format = [
            ('"ID"', 'FLOAT'),
            ('"Ticket"', 'STRING'),
            ('0.0', 'FLOAT'),
            ('1.0', 'FLOAT'),
            ('2.0', 'FLOAT'),
            ('3.0', 'FLOAT'),
            ('4.0', 'FLOAT'),
            ('5.0', 'FLOAT'),
            ('6.0', 'FLOAT'),
            ('7.0', 'FLOAT'),
            ('8.0', 'FLOAT'),
            ('9.0', 'FLOAT'),
            ('10.0', 'FLOAT'),
            ('11.0', 'FLOAT'),
            ('12.0', 'FLOAT'),
            ('13.0', 'FLOAT'),
            ('14.0', 'FLOAT'),
            ('15.0', 'FLOAT'),
            ('16.0', 'FLOAT'),
            ('17.0', 'FLOAT'),
            ('18.0', 'FLOAT'),
            ('19.0', 'FLOAT'),
            ('20.0', 'FLOAT'),
            ('21.0', 'FLOAT'),
            ('22.0', 'FLOAT'),
            ('23.0', 'FLOAT'),
            ('"Gesamt"\n', 'FLOAT')
        ]
        entries_3_sheet_format = [
            ('"ID"', 'FLOAT'),
            ('"Ticket"', 'STRING'),
            ('"0:00"', 'FLOAT'),
            ('"1:00"', 'FLOAT'),
            ('"2:00"', 'FLOAT'),
            ('"3:00"', 'FLOAT'),
            ('"4:00"', 'FLOAT'),
            ('"5:00"', 'FLOAT'),
            ('"6:00"', 'FLOAT'),
            ('"7:00"', 'FLOAT'),
            ('"8:00"', 'FLOAT'),
            ('"9:00"', 'FLOAT'),
            ('"10:00"', 'FLOAT'),
            ('"11:00"', 'FLOAT'),
            ('"12:00"', 'FLOAT'),
            ('"13:00"', 'FLOAT'),
            ('"14:00"', 'FLOAT'),
            ('"15:00"', 'FLOAT'),
            ('"16:00"', 'FLOAT'),
            ('"17:00"', 'FLOAT'),
            ('"18:00"', 'FLOAT'),
            ('"19:00"', 'FLOAT'),
            ('"20:00"', 'FLOAT'),
            ('"21:00"', 'FLOAT'),
            ('"22:00"', 'FLOAT'),
            ('"23:00"', 'FLOAT'),
            ('"Gesamt"\n', 'FLOAT')
        ]
        self.check_format(output_mock,output_target,'bookings',bookings_format)
        self.check_format(output_mock,output_target,'orders',orders_format,'_1day')
        self.check_format(output_mock,output_target,'orders',orders_format)
        self.check_format(output_mock,output_target,'customers',customers_format)
        self.check_format_entries(output_mock,output_target,entries_0and2_sheet_format)
        self.check_format_entries(output_mock,output_target,entries_1_sheet_format,[1])
        self.check_format_entries(output_mock,output_target,entries_0and2_sheet_format,[2])
        self.check_format_entries(output_mock,output_target,entries_3_sheet_format,[3])

    def check_format(self, output_mock, output_target, report, expected_format, 
                     suffix='_7days'):
        output_mock.return_value = iter([output_target])
        FetchGomusReport(report=report, suffix=suffix).requires()
        FetchGomusReport(report=report, suffix=suffix).run()
        with output_target.open('r') as output_file:
            first_line = output_file.readline()
            columns = first_line.split(',')
            second_line = output_file.readline()
            data = second_line.replace('"', '').split(',')
            self.check(expected_format, columns, data)

    def check_format_entries(self, output_mock, output_target, expected_format, sheet=[0]):
        output_mock.return_value = iter([output_target])
        FetchGomusReport(report='entries', suffix='_1day', sheet_indices=sheet).requires()
        FetchGomusReport(report='entries', suffix='_1day', sheet_indices=sheet).run()
        with output_target.open('r') as output_file:
            first_line = output_file.readline()
            while '"ID"' not in first_line:
                first_line = output_file.readline()
            columns = first_line.split(',')
            second_line = output_file.readline()
            data = second_line.replace('"', '').split(',')
            self.check(expected_format, columns, data)

    def check(self, expected_format, columns, data):
        # i was looking for assertNotRaises() but it seems, 
        # like this doesn't exist in python
        for i in range(len(columns)):
            # check, if the cloumn has the right name
            self.assertEquals(columns[i],expected_format[i][0])
            # check if the data has the right format
            try:
                if data[i]=='':
                    pass
                elif expected_format[i][1]=='FLOAT':
                    float(data[i])
                elif expected_format[i][1]=='DATE':
                    dt.datetime.strptime(data[i], '%d.%m.%Y')
                elif expected_format[i][1]=='TIME':
                    dt.datetime.strptime(data[i], '%H:%M')
            except ValueError:
                self.assertTrue(False, f'The data in column '
                    f'{columns[i]} has the wrong format. "{data[i]}" is not '
                    f'from type {expected_format[i][1]}.')

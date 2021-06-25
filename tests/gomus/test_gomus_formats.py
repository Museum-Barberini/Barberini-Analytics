"""Tests the format of downloaded gomus stuff."""

import unittest
import datetime as dt
import os
import pandas as pd
from unittest.mock import patch

from luigi.format import UTF8
from luigi.mock import MockTarget

from db_test import DatabaseTestCase
from gomus._utils.fetch_report import FetchGomusReport, FetchEventReservations


class GomusFormatTest(DatabaseTestCase):
    """The abstract base class for gomus format tests."""

    def __init__(self, report, expected_format, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.report = report
        self.expected_format = expected_format

    def prepare_output_target(self, output_mock):
        self.output_target = MockTarget('data_out', format=UTF8)
        output_mock.return_value = [self.output_target]

    def fetch_gomus_report(self, suffix='_7days', sheet=[0]):
        self.run_task(FetchGomusReport(report=self.report,
                                       suffix=suffix,
                                       sheet_indices=sheet))

    def check_format(self, skiprows=0, skipfooter=0):
        with self.output_target.open('r') as output_file:
            df = pd.read_csv(output_file,
                             skipfooter=skipfooter,
                             skiprows=skiprows,
                             engine='python')

            for i in range(len(self.expected_format)):
                if df.columns[i] == 'Keine Daten vorhanden':
                    break
                # this checks whether the columns are named right
                self.assertEqual(df.columns[i],
                                 self.expected_format[i][0])
                df.apply(lambda x: self.check_type(
                    x[self.expected_format[i][0]],
                    self.expected_format[i][1]),
                    axis=1)

    def check_type(self, data, expected_type):
        # To check if the data in the columns has the right type,
        # we try to converte the string into the expected type and
        # catch a ValueError or TypeError, if something goes wrong.
        # As we don't process data from type "STRING"/ just store
        # it as text, we don't have to explicitly check the type.
        try:
            if data == '':
                pass
            elif expected_type == 'FLOAT':
                float(data)
            elif expected_type == 'DATE':
                dt.datetime.strptime(data, '%d.%m.%Y')
            elif expected_type == 'TIME':
                dt.datetime.strptime(data, '%H:%M')
        except (ValueError, TypeError):
            self.assertTrue(False, f'{data} is not from type {expected_type}')


class TestCustomersFormat(GomusFormatTest):

    def __init__(self, *args, **kwargs):
        super().__init__(
            'customers',
            [("Nummer", 'FLOAT'),
             ("Anrede", 'STRING'),
             ("Vorname", 'STRING'),
             ("Name", 'STRING'),
             ("E-Mail", 'STRING'),
             ("Telefon", 'STRING'),
             ("Mobil", 'STRING'),
             ("Fax", 'STRING'),
             ("Sprache", 'STRING'),
             ("Kategorie", 'STRING'),
             ("Straße", 'STRING'),
             ("PLZ", 'STRING'),
             ("Stadt", 'STRING'),
             ("Land", 'STRING'),
             ("Typ", 'STRING'),
             ("Erstellt am", 'DATE'),
             ("Newsletter", 'STRING'),
             ("Jahreskarte", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    @unittest.skipUnless(
        os.getenv('FULL_TEST') == 'True', 'long running test')
    def test_customers_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report()
        self.check_format()


class TestBookingsFormat(GomusFormatTest):

    def __init__(self, *args, **kwargs):
        super().__init__(
            'bookings',
            [("Buchung", 'FLOAT'),
             ("Datum", 'DATE'),
             ("Uhrzeit von", 'TIME'),
             ("Uhrzeit bis", 'TIME'),
             ("Museum", 'STRING'),
             ("Ausstellung", 'STRING'),
             ("Raum", 'STRING'),
             ("Treffpunkt", 'STRING'),
             ("Angebotskategorie", 'STRING'),
             ("Angebot/Termin", 'STRING'),
             ("Teilnehmerzahl", 'FLOAT'),
             ("Guide", 'STRING'),
             ("Kunde", 'STRING'),
             ("E-Mail", 'STRING'),
             ("Institution", 'STRING'),
             ("Notiz (aus Kundendatensatz)", 'STRING'),
             ("Führungsentgelt / Summe Endkundenpreis ohne Storno", 'STRING'),
             ("Honorar", 'STRING'),
             ("Zahlungsart", 'STRING'),
             ("Kommentar (Interner Hinweis)", 'STRING'),
             ("Status", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_bookings_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report()
        self.check_format()


class TestOrdersFormat(GomusFormatTest):

    def __init__(self, *args, **kwargs):
        super().__init__(
            'orders',
            [("Bestellnummer", 'FLOAT'),
             ("Erstellt", 'FLOAT'),
             ("Kundennummer", 'FLOAT'),
             ("Kunde", 'STRING'),
             ("Bestellwert", 'STRING'),
             ("Gutscheinwert", 'STRING'),
             ("Gesamtbetrag", 'STRING'),
             ("ist gültig?", 'STRING'),
             ("Bezahlstatus", 'STRING'),
             ("Bezahlmethode", 'STRING'),
             ("ist storniert?", 'STRING'),
             ("Herkunft", 'STRING'),
             ("Kostenstellen", 'STRING'),
             ("In Rechnung gestellt am", 'STRING'),
             ("Rechnungen", 'STRING'),
             ("Korrekturrechnungen", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_orders_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day')
        self.check_format()


class TestEntriesSheet0Format(GomusFormatTest):
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)

    def __init__(self, *args, **kwargs):
        super().__init__(
            'entries',
            [("ID", 'FLOAT'),
             ("Ticket", 'STRING'),
             (self.yesterday.strftime("%d.%m.%Y"), 'STRING'),
             (self.today.strftime("%d.%m.%Y"), 'STRING'),
             ("Gesamt", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_entries_sheet0_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day', sheet=[0])
        try:
            self.check_format(skiprows=5, skipfooter=1)
        except AssertionError:
            self.check_format(skiprows=7, skipfooter=1)


class TestEntriesSheet1Format(GomusFormatTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            'entries',
            [("ID", 'FLOAT'),
             ("Ticket", 'STRING'),
             *[(str(float(i)), 'FLOAT') for i in range(24)],
             ("Gesamt", 'FLOAT')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_entries_sheet1_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day', sheet=[1])
        self.check_format(skiprows=0, skipfooter=1)


class TestEntriesSheet3Format(GomusFormatTest):

    def __init__(self, *args, **kwargs):
        super().__init__(
            'entries',
            [("ID", 'FLOAT'),
             ("Ticket", 'STRING'),
             *[(f'{i}:00', 'FLOAT') for i in range(24)],
             ("Gesamt", 'FLOAT')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_entries_sheet3_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day', sheet=[3])
        self.check_format(skiprows=0, skipfooter=1)


class TestEventsFormat(GomusFormatTest):
    """Tests the FetchEventReservations task."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            'events',
            [("Id", 'FLOAT'),
             ("Name", 'STRING'),
             ("Nachname", 'STRING'),
             ("Plätze", 'FLOAT'),
             ("Preis", 'FLOAT'),
             ("Datum", 'FLOAT'),
             ("Zeit", 'FLOAT'),
             ("Kommentar", 'STRING'),
             ("Anschrift", 'STRING'),
             ("Telefon", 'STRING'),
             ("Mobil", 'STRING'),
             ("E-Mail", 'STRING'),
             ("Teilnehmerinfo", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchEventReservations, 'output')
    def test_events_format(self, output_mock):
        self.output_target = MockTarget('data_out', format=UTF8)
        output_mock.return_value = self.output_target
        self.run_task(FetchEventReservations(12345))
        self.check_format(skiprows=5, skipfooter=1)

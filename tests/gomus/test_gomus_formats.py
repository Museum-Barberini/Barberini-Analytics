from unittest.mock import patch
import datetime as dt
import pandas as pd

from luigi.format import UTF8
from luigi.mock import MockTarget

from gomus._utils.fetch_report import FetchGomusReport, FetchEventReservations
from task_test import DatabaseTaskTest


class GomusFormatTest(DatabaseTaskTest):
    def __init__(self, report, expected_format, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.report = report
        self.expected_format = expected_format

    def prepare_output_target(self, output_mock):
        self.output_target = MockTarget('data_out', format=UTF8)
        output_mock.return_value = iter([self.output_target])

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
                # this checks, if the colums are named right
                self.assertEqual(df.columns[i],
                                 self.expected_format[i][0])
                df.apply(lambda x: self.check_type(
                    x[self.expected_format[i][0]],
                    self.expected_format[i][1]),
                    axis=1)

    def check_type(self, data, expected_type):
        # to check, if the date in the columns has the right type,
        # we try to converte the string into the expected type and
        # catch a ValueError, if something goes wrong
        try:
            if data == '':
                pass
            elif expected_type == 'FLOAT':
                float(data)
            elif expected_type == 'DATE':
                dt.datetime.strptime(data, '%d.%m.%Y')
            elif expected_type == 'TIME':
                dt.datetime.strptime(data, '%H:%M')
        except ValueError:
            self.assertTrue(False, f'{data} is not from type {expected_type}')


class TestCustomersFormat(GomusFormatTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            'customers', [
            ("Nummer", 'FLOAT'),
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
    def test_customers_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report()
        self.check_format()


class TestBookingsFormat(GomusFormatTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            'bookings', [
            ("Buchung", 'FLOAT'),
            ("Datum", 'DATE'),
            ("Uhrzeit von", 'TIME'),
            ("Uhrzeit bis", 'TIME'),
            ("Museum", 'STRING'),
            ("Ausstellung", 'STRING'),
            ("Raum", 'STRING'),
            ("Treffpunkt", 'STRING'),
            ("Angebotskategorie", 'STRING'),
            ("Titel", 'STRING'),
            ("Teilnehmerzahl", 'FLOAT'),
            ("Guide", 'STRING'),
            ("Kunde", 'STRING'),
            ("E-Mail", 'STRING'),
            ("Institution", 'STRING'),
            ("Notiz", 'STRING'),
            ("Führungsentgelt / Summe Endkundenpreis ohne Storno", 'STRING'),
            ("Honorar", 'STRING'),
            ("Zahlungsart", 'STRING'),
            ("Kommentar", 'STRING'),
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
            'orders', [
            ("Bestellnummer", 'FLOAT'),
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
            'entries', [
            ("ID", 'FLOAT'),
            ("Ticket", 'STRING'),
            (self.yesterday.strftime("%d.%m.%Y"), 'STRING'),
            (self.today.strftime("%d.%m.%Y"), 'STRING'),
            ("Gesamt", 'STRING')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_entries_sheet0_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day', sheet=[0])
        self.check_format(skiprows=5, skipfooter=1)


class TestEntriesSheet1Format(GomusFormatTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            'entries', [
            ("ID", 'FLOAT'),
            ("Ticket", 'STRING'),
            ("0.0", 'FLOAT'),
            ("1.0", 'FLOAT'),
            ("2.0", 'FLOAT'),
            ("3.0", 'FLOAT'),
            ("4.0", 'FLOAT'),
            ("5.0", 'FLOAT'),
            ("6.0", 'FLOAT'),
            ("7.0", 'FLOAT'),
            ("8.0", 'FLOAT'),
            ("9.0", 'FLOAT'),
            ("10.0", 'FLOAT'),
            ("11.0", 'FLOAT'),
            ("12.0", 'FLOAT'),
            ("13.0", 'FLOAT'),
            ("14.0", 'FLOAT'),
            ("15.0", 'FLOAT'),
            ("16.0", 'FLOAT'),
            ("17.0", 'FLOAT'),
            ("18.0", 'FLOAT'),
            ("19.0", 'FLOAT'),
            ("20.0", 'FLOAT'),
            ("21.0", 'FLOAT'),
            ("22.0", 'FLOAT'),
            ("23.0", 'FLOAT'),
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
            'entries', [
            ("ID", 'FLOAT'),
            ("Ticket", 'STRING'),
            ("0:00", 'FLOAT'),
            ("1:00", 'FLOAT'),
            ("2:00", 'FLOAT'),
            ("3:00", 'FLOAT'),
            ("4:00", 'FLOAT'),
            ("5:00", 'FLOAT'),
            ("6:00", 'FLOAT'),
            ("7:00", 'FLOAT'),
            ("8:00", 'FLOAT'),
            ("9:00", 'FLOAT'),
            ("10:00", 'FLOAT'),
            ("11:00", 'FLOAT'),
            ("12:00", 'FLOAT'),
            ("13:00", 'FLOAT'),
            ("14:00", 'FLOAT'),
            ("15:00", 'FLOAT'),
            ("16:00", 'FLOAT'),
            ("17:00", 'FLOAT'),
            ("18:00", 'FLOAT'),
            ("19:00", 'FLOAT'),
            ("20:00", 'FLOAT'),
            ("21:00", 'FLOAT'),
            ("22:00", 'FLOAT'),
            ("23:00", 'FLOAT'),
            ("Gesamt", 'FLOAT')],
            *args, **kwargs)

    @patch.object(FetchGomusReport, 'output')
    def test_entries_sheet3_format(self, output_mock):
        self.prepare_output_target(output_mock)
        self.fetch_gomus_report(suffix='_1day', sheet=[3])
        self.check_format(skiprows=0, skipfooter=1)


class TestEventsFormat(GomusFormatTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            'events', [
            ("Id", 'FLOAT'),
            ("Kunde", 'STRING'),
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
        FetchEventReservations(123).run()
        self.check_format(skiprows=5, skipfooter=1)

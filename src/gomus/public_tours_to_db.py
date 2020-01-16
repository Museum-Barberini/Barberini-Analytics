#!/usr/bin/env python3
import csv
import luigi
import pandas as pd
import psycopg2

from csv_to_db import CsvToDb
from luigi.format import UTF8
from gomus.bookings_to_db import BookingsToDB
from gomus_report import FetchGomusReport, FetchTourReservations
from extract_gomus_data import hash_booker_id
from set_db_connection_options import set_db_connection_options
from xlrd import xldate_as_datetime

class PublicToursToDB(CsvToDb):

        table = 'gomus_public_tour'

        columns = [
                ('id', 'INT'),
                ('booker_id', 'INT'),
                ('tour_id', 'INT'),
                ('reservation_count', 'INT'),
                ('order_date', 'DATE'),
                ('status', 'TEXT')
        ]

        primary_key = 'id'

        def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                set_db_connection_options(self)
        
        def requires(self):
                return ExtractPublicTourData(columns=[el[0] for el in self.columns])

class ExtractPublicTourData(luigi.Task):
        columns = luigi.parameter.ListParameter(description="Column names")
        
        seed = luigi.parameter.IntParameter(description="Seed to use for hashing", default=666)
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.public_tours_list = None
            self.public_tours_df = None

        def requires(self):
                return EnsureBookingsIsRun()

        def output(self):
                return luigi.LocalTarget('output/gomus/public_tours.csv', format=UTF8)

        def run(self):
                self.public_tours_list = luigi.task.flatten(self.input())
                self.public_tours_df = pd.DataFrame(columns=self.columns)
                for i in range(0, len(self.flat), 2):
                        self.append_tour_data(i, 'Gebucht')
                        self.append_tour_data(i+1, 'Storniert')

                with self.output().open('w') as output_csv:
                        self.public_tours_df.to_csv(output_csv, index=False)    

        def append_tour_data(self, index, status):
                with self.public_tours_list[index].open('r') as sheet:
                        sheet_reader = csv.reader(sheet)
                        tour_id = int(float(next(sheet_reader)[0]))

                tour_df = pd.read_csv(self.flat[index].path, skiprows=5)
                tour_df['Status'] = status
                tour_df['Tour_id'] = tour_id
                tour_df = tour_df.filter([
                        'Id', 'E-Mail', 'Tour_id', 'Plätze', 'Datum', 'Status'
                ])

                tour_df.columns = self.columns

                tour_df['id'] = tour_df['id'].apply(int)
                tour_df['booker_id'] = tour_df['booker_id'].apply(hash_booker_id, args=(self.seed,))
                tour_df['reservation_count'] = tour_df['reservation_count'].apply(int)
                tour_df['order_date'] = tour_df['order_date'].apply(self.float_to_datetime)

                self.df = self.df.append(tour_df)

        def float_to_datetime(self, string):
                return xldate_as_datetime(float(string), 0).date()


class EnsureBookingsIsRun(luigi.Task):
        def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                set_db_connection_options(self)
                self.output_list = []
                self.is_complete = False
                self.row_list = []

        def run(self):
                try:
                        conn = psycopg2.connect(
                                host=self.host, database=self.database,
                                user=self.user, password=self.password
                        )
                        cur = conn.cursor()
                        query = 'SELECT id FROM gomus_booking WHERE category=\'Öffentliche Führung\''
                        cur.execute(query)

                        row = cur.fetchone()
                        while row is not None:
                                tour_id = row[0]
                                if not tour_id in self.row_list:
                                        approved = yield FetchTourReservations(tour_id, 0)
                                        cancelled = yield FetchTourReservations(tour_id, 1)
                                        self.output_list.append(approved)
                                        self.output_list.append(cancelled)
                                        self.row_list.append(tour_id)
                                row = cur.fetchone()
                        
                        self.is_complete = True
                
                except psycopg2.DatabaseError as error:
                        print(error)
                        exit(1)
                
                finally:
                        if conn is not None:
                                conn.close()
        
        def output(self):
                return self.output_list
        
        def complete(self):
                return self.is_complete

        def requires(self):
                yield BookingsToDB()

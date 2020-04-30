import datetime as dt
import luigi
import pandas as pd
import re
from luigi.format import UTF8

from gomus.customers import CustomersToDB
from data_preparation_task import DataPreparationTask
from db_connector import db_connector


class CleansePostalCodes(DataPreparationTask):
    today = luigi.parameter.DateParameter(default=dt.datetime.today())
    skip_count = 0

    def requires(self):
        return CustomersToDB(today=self.today)

    def output(self):
        return luigi.LocalTarget('output/gomus/cleansed_customers.csv',
                                 format=UTF8)

    def run(self):
        customer_df = self.get_customer_data()

        customer_df['cleansed_postal_code'] = ''
        customer_df['cleansed_postal_code'] = customer_df.apply(
                lambda x: self.match_postal_code(
                    postal_code=x['postal_code'],
                    country=x['country']
                ), axis=1)

        total_count = self.get_total_count()
        print('-------------------------------------------------')
        print(f'Skipped {self.skip_count} out of {total_count} postal codes')
        print('Percentage:', '{0:.0%}'.format(self.skip_count/total_count))
        print('-------------------------------------------------')

    def get_customer_data(self):
        customer_data = db_connector.query(
            query='SELECT * FROM gomus_customer')

        columns = db_connector.query(
            query='''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name=\'gomus_customer\'
                ORDER BY ordinal_position asc
            ''')

        customer_df = pd.DataFrame(customer_data,
                                   columns=[col[0] for col in columns])
        return customer_df

    def get_total_count(self):
        total_count = db_connector.query(
            query='SELECT COUNT(*) FROM gomus_customer',
            only_first=True
        )[0]
        return total_count

    def match_postal_code(self, postal_code, country):
        if country == 'Deutschland' or country is None:
            matching_codes = re.findall(
                r'(?!01000|99999)(0[1-9]\d{3}|[1-9]\d{4})', str(postal_code))
            if len(matching_codes) == 1:
                return matching_codes[0]

        self.skip_count += 1
        return None

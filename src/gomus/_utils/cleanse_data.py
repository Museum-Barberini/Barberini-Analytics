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
    none_count = 0

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

        total_count = db_connector.query(
            query='SELECT COUNT(*) FROM gomus_customer',
            only_first=True
        )[0]

        print('-------------------------------------------------')
        print(f'Skipped {self.skip_count} out of {total_count} postal codes')
        print('Percentage:', '{0:.0%}'.format(self.skip_count/total_count))
        print()
        print('{0:.0%}'.format(self.none_count/total_count),
              'of all values are empty. ({})'.format(self.none_count))
        print()
        print(' =>', self.skip_count-self.none_count,
              'values were not validated.')
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

    def match_postal_code(self, postal_code, country):

        if not postal_code:
            self.none_count += 1

        cleansed_postal_code = str(postal_code)
        cleansed_postal_code = cleansed_postal_code.replace('^', '')
        cleansed_postal_code = cleansed_postal_code.replace(' ', '')

        country_func = self.get_validation_function(country)
        if country_func:
            result = country_func(cleansed_postal_code)
            if country_func == self.validate_US:
                if not result:
                    print(result, cleansed_postal_code)
            if not result:
                self.skip_count += 1
            return result
        else:
            self.skip_count += 1
            return None

    def get_validation_function(self, country):
        country_to_func = {
            'Deutschland': self.validate_DE,
            'Schweiz': self.validate_CH,
            'Vereinigtes Königreich': self.validate_GB,
            'Vereinigte Staaten von Amerika': self.validate_US,
            None: self.validate_DE
        }
        return country_to_func.get(country)

    def validate_DE(self, postal_code):
        if len(re.findall(
            r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-_.]))\d{4}(?=$|\s|[a-zA-Z])',
            postal_code)
        ):
            postal_code = '0' + postal_code

        matching_codes = re.findall(
            r'(?!01000|99999)(0[1-9]\d{3}|[1-9]\d{4})',
            postal_code)

        if len(matching_codes):
            return matching_codes[0]
        else:
            return None

    def validate_CH(self, postal_code):
        matches = re.findall(
            r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-_.]))[1-9]\d{3}(?=$|\s|[a-zA-Z])',
            postal_code
        )
        if len(matches):
            return matches[0]
        return None

    def validate_GB(self, postal_code):
        matches = re.findall(
            r'([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z]'
            r'[A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z]'
            r'[A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})',
            postal_code
        )
        if len(matches):
            return matches[0]
        return None

    def validate_US(self, postal_code):
        if len(re.findall(
            r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-_.]))\d{4}(?=$|\s|[a-zA-Z])',
            postal_code)
        ):
            postal_code = '0' + postal_code

        matches = re.findall(
            r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-_.]))([0-9]{5}(?:-[0-9]{4})?)|'
            r'[0-9]{9}(?=$|\s|[a-zA-Z])',
            postal_code
        )
        if len(matches):
            return matches[0]
        return None

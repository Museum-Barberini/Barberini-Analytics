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
    other_country_count = 0

    common_lookahead = r'(?=$|\s|[a-zA-Z])'
    common_lookbehind = r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-_.]))'

    def requires(self):
        return CustomersToDB(today=self.today)

    def output(self):
        return luigi.LocalTarget('output/gomus/cleansed_customers.csv',
                                 format=UTF8)

    def run(self):
        customer_df = self.get_customer_data()

        customer_df['cleansed_postal_code'] = None
        customer_df['cleansed_country'] = None

        customer_df['cleansed_postal_code'],
        customer_df['cleansed_country'] = customer_df.apply(
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
        print()
        print('Count of other (less common, not validated) countries:',
              self.other_country_count)
        print('-------------------------------------------------')

        with self.output().open('w') as output_csv:
            customer_df.to_csv(output_csv, index=False, header=True)

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

        result_postal = None

        if not postal_code:
            self.none_count += 1
            self.skip_count += 1
            return None

        cleansed_postal_code = \
            self.replace_rare_symbols(str(postal_code))

        country_to_function = {
            'Deutschland': self.validate_DE,
            'Schweiz': self.validate_CH,
            'Vereinigtes Königreich': self.validate_GB,
            'Vereinigte Staaten von Amerika': self.validate_US,
            'Frankreich': self.validate_FR,
            'Niederlande': self.validate_NL,
            'Österreich': self.validate_AT,
            'Polen': self.validate_PL,
            'Britische Jungferninseln': self.validate_GB,
            'United States Minor Outlying Islands': self.validate_US
        }
        country_func = country_to_function.get(country)

        if country_func:
            result_postal = country_func(cleansed_postal_code)
            result_country = country

        for key, func in country_to_function.items():
            if not result_postal:
                result_postal = func(cleansed_postal_code)
                result_country = key

        if not result_postal:
            self.skip_count += 1
            result_country = country

        if country and not country_func:
            # we have countries that we can't check yet - let us count them
            self.other_country_count += 1

        return result_postal, result_country

    def replace_rare_symbols(self, postal_code):

        replacements = {
            '!': '1',
            '"': '2',
            '§': '3',
            '$': '4',
            '%': '5',
            '&': '6',
            '/': '7',
            '(': '8',
            ')': '9',
            '=': '0',
            '^': '',
            '+': '',
            '*': '',
            ' ': '',
            '´': '',
            ',': '',
            '.': '',
            '_': '',
            '-': '',
            '@': '',
            '?': '0',
            'ß': '0'
        }

        converted_postal_code = \
            postal_code.translate(str.maketrans(replacements))

        return converted_postal_code

    def add_zeroes(self, postal_code, digit_count):
        not_null_part = None

        for num in reversed(range(0, digit_count)):
            if not not_null_part:
                not_null_part = re.findall(
                    self.common_lookbehind +
                    rf'\d{{{num + 1}}}' +
                    self.common_lookahead,
                    postal_code)
                null_count = digit_count - (num + 1)

        if not_null_part:
            not_null_part = str(not_null_part[0])
            return null_count * '0' + not_null_part
        else:
            return postal_code

    def validate_DE(self, postal_code):

        new_postal_code = self.add_zeroes(postal_code, 5)

        matching_codes = re.findall(
            self.common_lookbehind +
            r'(?!01000|99999)(0[1-9]\d{3}|[1-9]\d{4})' +
            self.common_lookahead,
            new_postal_code)

        if len(matching_codes):
            return matching_codes[0]
        else:
            return None

    def validate_CH(self, postal_code):

        new_postal_code = self.add_zeroes(postal_code, 4)

        matches = re.findall(
            self.common_lookbehind + r'[1-9]\d{3}' + self.common_lookahead,
            new_postal_code
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

        new_postal_code = self.add_zeroes(postal_code, 5)

        matches = re.findall(
            self.common_lookbehind +
            r'([0-9]{5}(?:-[0-9]{4})?)|[0-9]{9}' +
            self.common_lookahead,
            new_postal_code
        )
        if len(matches):
            return matches[0]
        return None

    def validate_FR(self, postal_code):

        new_postal_code = self.add_zeroes(postal_code, 5)

        matches = re.findall(
            self.common_lookbehind +
            r'(?:[0-8]\d|9[0-8])\d{3}' +
            self.common_lookahead,
            new_postal_code
        )
        if len(matches):
            return matches[0]

        return None

    def validate_NL(self, postal_code):

        matches = re.findall(
            r'[1-9][0-9]{3}?(?!sa|sd|ss)[a-zA-Z]{2}',
            postal_code
        )
        if len(matches):
            return matches[0]
        return None

    def validate_AT(self, postal_code):

        new_postal_code = self.add_zeroes(postal_code, 4)

        matches = re.findall(
            self.common_lookbehind + r'\d{4}' + self.common_lookahead,
            new_postal_code
        )
        if len(matches):
            return matches[0]
        return None

    def validate_PL(self, postal_code):

        matches = re.findall(
            self.common_lookbehind +
            r'([0-9]{2})((-|)[0-9]{3})?' +
            self.common_lookahead,
            postal_code
        )
        if len(matches):
            return matches[0]

        else:
            new_postal_code = self.add_zeroes(postal_code, 5)
            new_matches = re.findall(
                self.common_lookbehind +
                r'([0-9]{2})((-|)[0-9]{3})?' +
                self.common_lookahead,
                new_postal_code
            )
            if len(new_matches):
                return new_matches[0]

        return None

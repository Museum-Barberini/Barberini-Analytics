import datetime as dt
import luigi
import pandas as pd
import re
from luigi.format import UTF8

from gomus.customers import CustomersToDB
from data_preparation_task import DataPreparationTask
from db_connector import db_connector
from _utils.german_postal_codes import GermanPostalCodes


COUNTRY_TO_DATA = {
    'Deutschland':
        ['DE', 5, r'(?!01000|99999)(0[1-9]\d{3}|[1-9]\d{4})'],
    'Schweiz':
        ['CH', 4, r'[1-9]\d{3}'],
    'Vereinigtes Königreich':
        ['UK', 0, r'([A-Za-z][A-Ha-hJ-Yj-y]?[0-9][A-Za-z0-9]'
            r'? ?[0-9][A-Za-z]{2}|[Gg][Ii][Rr] ?0[Aa]{2})'],
    'Vereinigte Staaten von Amerika':
        ['US', 5, r'([0-9]{5}(?:[0-9]{4})?)'],
    'Frankreich':
        ['FR', 5, r'(?:[0-8]\d|9[0-8])\d{3}'],
    'Niederlande':
        ['NL', 0, r'[1-9][0-9]{3}?(?!sa|sd|ss)[a-zA-Z]{2}'],
    'Österreich':
        ['AT', 4, r'\d{4}'],
    'Polen':
        ['PL', 5, r'([0-9]{2}\-[0-9]{3})|[0-9]{5}'],
    'Belgien':
        ['BE', 4, r'[1-9]\d{3}'],
    'Dänemark':
        ['DK', 4, r'[1-9]\d{3}'],
    'Italien':
        ['IT', 5, r'\d{5}'],
    'Russische Föderation':
        ['RU', 0, r'\d{6}'],
    'Schweden':
        ['SE', 5, r'\d{3}\s*\d{2}'],
    'Spanien':
        ['ES', 5, r'(?:0[1-9]|[1-4]\d|5[0-2])\d{3}'],
    'Britische Jungferninseln':
        ['UK', 0, r'([A-Za-z][A-Ha-hJ-Yj-y]?[0-9][A-Za-z0-9]'
            r'? ?[0-9][A-Za-z]{2}|[Gg][Ii][Rr] ?0[Aa]{2})'],
    'United States Minor Outlying Islands':
        ['US', 5, r'([0-9]{5}(?:[0-9]{4})?)']
}


class CleansePostalCodes(DataPreparationTask):

    # runs about 60 mins - 2 hours should suffice
    worker_timeout = 7200

    today = luigi.parameter.DateParameter(default=dt.datetime.today())
    skip_count = 0
    none_count = 0
    other_country_count = 0
    cleansed_count = 0
    last_percentage = 0
    german_postal_df = None

    common_lookahead = r'(?=$|\s|[a-zA-Z])'
    common_lookbehind = r'(?:(?<=^)|(?<=\s)|(?<=[a-zA-Z-]))'

    def requires(self):
        yield GermanPostalCodes()
        yield CustomersToDB(today=self.today)

    def output(self):
        return luigi.LocalTarget('output/gomus/cleansed_customers.csv',
                                 format=UTF8)

    def run(self):
        customer_df = self.get_customer_data()

        with self.input()[0].open('r') as postal_csv:
            self.german_postal_df = \
                pd.read_csv(postal_csv, encoding='utf-8', dtype=str)

        customer_df['cleansed_postal_code'] = None
        customer_df['cleansed_country'] = None

        self.total_count = len(customer_df)

        customer_df['cleansed_postal_code'],
        customer_df['cleansed_country'] = customer_df.apply(
                lambda x: self.match_postal_code(
                    postal_code=x['postal_code'],
                    country=x['country'],
                    customer_id=x['customer_id']
                ), axis=1)

        print()
        print('-------------------------------------------------')
        print(f'Skipped {self.skip_count} of {self.total_count} postal codes')
        print('Percentage:',
              '{0:.0%}'.format(self.skip_count/self.total_count))
        print()
        print('{0:.0%}'.format(self.none_count/self.total_count),
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

        query_limit = 'LIMIT 10' if self.minimal_mode else ''

        customer_data = db_connector.query(
            query=f'SELECT * FROM gomus_customer {query_limit}')

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

    def match_postal_code(self, postal_code, country, customer_id):

        result_postal = None

        if not postal_code:
            self.none_count += 1
            self.skip_count += 1
            return None

        cleansed_code = self.replace_rare_symbols(str(postal_code))

        country_data = COUNTRY_TO_DATA.get(country)

        if country_data:
            result_postal = \
                self.validate_country(cleansed_code, *country_data)
            result_country = country

        for key, data in COUNTRY_TO_DATA.items():
            if not result_postal:
                result_postal = \
                    self.validate_country(cleansed_code, *data)
                result_country = key

        if not result_postal:
            self.skip_count += 1
            result_country = country
            return result_postal, country

        if country and not country_data:
            # we have countries that we can't check yet - let us count them
            self.other_country_count += 1

        db_connector.execute(
            (f'''
                UPDATE gomus_customer SET cleansed_postal_code
                =\'{result_postal}\',cleansed_country=\'{result_country}\'
                WHERE customer_id={customer_id}
            '''))

        self.cleansed_count += 1
        percentage = int(round(self.cleansed_count/self.total_count*100))

        if self.last_percentage < percentage:
            print(f"\r{percentage}% cleansed ({self.cleansed_count})",
                  end='',
                  flush=True)
            self.last_percentage = percentage

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
            ':': '',
            ';': '',
            '_': '',
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

    def validate_country(self, postal_code, country_code, zeroes, regex):

        new_postal_code = postal_code

        if zeroes:
            if country_code == 'PL':
                perfect_matches = re.findall(
                    self.common_lookbehind + regex + self.common_lookahead,
                    postal_code)
                if not len(perfect_matches):
                    postal_code = postal_code.replace('-', '')
                    new_postal_code = self.add_zeroes(postal_code, zeroes)
            else:
                new_postal_code = self.add_zeroes(postal_code, zeroes)

        matching_codes = re.findall(
            self.common_lookbehind + regex + self.common_lookahead,
            new_postal_code)

        if len(matching_codes):
            result_code = matching_codes[0]
            if country_code == 'DE':
                if not(self.german_postal_df[
                       self.german_postal_df[
                            'Plz'].str.contains(result_code)].empty):
                    return result_code
            else:
                return result_code
        return None

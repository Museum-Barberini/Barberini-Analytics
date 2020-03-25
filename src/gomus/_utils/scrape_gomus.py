import csv
import os
import re
import time

import datetime as dt
import dateparser
import luigi
import pandas as pd
import psycopg2
import requests
from luigi.format import UTF8
from lxml import html

from data_preparation_task import DataPreparationTask
from gomus.customers import hash_id
from gomus.orders import OrdersToDB
from gomus._utils.extract_bookings import ExtractGomusBookings
from set_db_connection_options import set_db_connection_options


class FetchGomusHTML(luigi.Task):
    url = luigi.parameter.Parameter(description="The URL to fetch")

    def output(self):
        name = 'output/gomus/html/' + \
            self.url. \
            replace('http://', ''). \
            replace('https://', ''). \
            replace('/', '_'). \
            replace('.', '_') + \
            '.html'

        return luigi.LocalTarget(name, format=UTF8)

    # simply wait for a moment before requesting, as we don't want to
    # overwhelm the server with our interest in classified information...
    def run(self):
        time.sleep(0.2)
        response = requests.get(
            self.url,
            cookies=dict(
                _session_id=os.environ['GOMUS_SESS_ID']))
        response.raise_for_status()

        with self.output().open('w') as html_out:
            html_out.write(response.text)


# inherit from this if you want to scrape gomus (it might be wise to have
# a more general scraper class if we need to scrape something other than
# gomus)
class GomusScraperTask(DataPreparationTask):
    base_url = "https://barberini.gomus.de"

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def extract_from_html(self, base_html, xpath):
        try:
            return html.tostring(base_html.xpath(
                xpath)[0], method='text', encoding="unicode")
        except IndexError:
            return ""


class FetchBookingsHTML(luigi.Task):
    timespan = luigi.parameter.Parameter(default='_nextYear')
    base_url = luigi.parameter.Parameter(
        description="Base URL to append bookings IDs to")
    minimal = luigi.parameter.BoolParameter(default=False)
    columns = luigi.parameter.ListParameter(description="Column names")

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
        self.output_list = []

    def requires(self):
        return ExtractGomusBookings(timespan=self.timespan,
                                    minimal=self.minimal,
                                    columns=self.columns)

    def output(self):
        return luigi.LocalTarget('output/gomus/bookings_htmls.txt')

    def run(self):
        if self.minimal:
            bookings = pd.read_csv(self.input().path, nrows=5)
        else:
            bookings = pd.read_csv(self.input().path)

        db_booking_rows = []

        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )

            cur = conn.cursor()

            cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables"
                        f" WHERE table_name=\'gomus_booking\')")

            today_time = dt.datetime.today() - dt.timedelta(weeks=5)
            if cur.fetchone()[0]:
                query = (f'SELECT booking_id FROM gomus_booking'
                         f' WHERE start_datetime < \'{today_time}\'')

                cur.execute(query)
                db_booking_rows = cur.fetchall()

        finally:
            if conn is not None:
                conn.close()

        for i, row in bookings.iterrows():
            booking_id = row['booking_id']

            booking_in_db = False
            for db_row in db_booking_rows:
                if db_row[0] == booking_id:
                    booking_in_db = True
                    break

            if not booking_in_db:
                booking_url = self.base_url + str(booking_id)

                html_target = yield FetchGomusHTML(booking_url)
                self.output_list.append(html_target.path)

        with self.output().open('w') as html_files:
            html_files.write('\n'.join(self.output_list))


class FetchOrdersHTML(luigi.Task):
    base_url = luigi.parameter.Parameter(
        description="Base URL to append order IDs to")

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)
        self.output_list = []
        self.order_ids = [order_id[0] for order_id in self.get_order_ids()]

    def requires(self):
        return OrdersToDB()

    def output(self):
        return luigi.LocalTarget('output/gomus/orders_htmls.txt')

    def get_order_ids(self):
        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )
            cur = conn.cursor()

            cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables"
                        f" WHERE table_name=\'gomus_order_contains\')")

            if cur.fetchone()[0]:
                query = (f'SELECT order_id FROM gomus_order WHERE order_id '
                         'NOT IN (SELECT order_id FROM gomus_order_contains)')
                cur.execute(query)
                order_ids = cur.fetchall()

            else:
                query = (f'SELECT order_id FROM gomus_order')
                cur.execute(query)
                order_ids = cur.fetchall()

            return order_ids

        finally:
            if conn is not None:
                conn.close()

    def run(self):
        for i in range(len(self.order_ids)):

            url = self.base_url + str(self.order_ids[i])

            html_target = yield FetchGomusHTML(url)
            self.output_list.append(html_target.path)

        with self.output().open('w') as html_files:
            html_files.write('\n'.join(self.output_list))


class EnhanceBookingsWithScraper(GomusScraperTask):
    minimal = luigi.parameter.BoolParameter(default=False)
    columns = luigi.parameter.ListParameter(description="Column names")
    timespan = luigi.parameter.Parameter(default='_nextYear')

    # could take up to an hour to scrape all bookings in the next year
    worker_timeout = 3600

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        yield ExtractGomusBookings(timespan=self.timespan,
                                   minimal=self.minimal,
                                   columns=self.columns)
        yield FetchBookingsHTML(timespan=self.timespan,
                                base_url=self.base_url + '/admin/bookings/',
                                minimal=self.minimal,
                                columns=self.columns)

    def output(self):
        return luigi.LocalTarget('output/gomus/bookings.csv', format=UTF8)

    def run(self):
        if self.minimal:
            bookings = pd.read_csv(self.input()[0].path)
            bookings = bookings.head(5)
        else:
            bookings = pd.read_csv(self.input()[0].path)

        bookings['order_date'] = None
        bookings['language'] = ""
        bookings['customer_id'] = 0
        enhanced_bookings = pd.DataFrame(columns=self.columns)

        with self.input()[1].open('r') as all_htmls:
            for i, html_path in enumerate(all_htmls):
                html_path = html_path.replace('\n', '')
                with open(html_path,
                          'r',
                          encoding='utf-8') as html_file:
                    res_details = html_file.read()
                tree_details = html.fromstring(res_details)

                row = bookings.iloc[[i]]

                # firefox says the the xpath starts with //body/div[3]/ but we
                # apparently need div[2] instead
                booking_details = tree_details.xpath(
                    '//body/div[2]/div[2]/div[3]'
                    '/div[4]/div[2]/div[1]/div[3]')[0]

                # Order Date
                # .strip() removes \n in front of and behind string
                raw_order_date = self.extract_from_html(
                    booking_details,
                    'div[1]/div[2]/small/dl/dd[2]').strip()
                row['order_date'] = dateparser.parse(raw_order_date)

                # Language
                row['language'] = self.extract_from_html(
                    booking_details, 'div[3]/div[1]/dl[2]/dd[1]').strip()

                try:
                    customer_details = tree_details.xpath(
                        '/html/body/div[2]/div[2]/div[3]/'
                        'div[4]/div[2]/div[2]/div[2]')[0]

                    # Customer E-Mail (not necessarily same as in report)
                    customer_mail = self.extract_from_html(
                        customer_details,
                        'div[1]/div[1]/div[2]/small[1]').strip().split('\n')[0]

                    if re.match(r'^\S+@\S+\.\S+$', customer_mail):
                        row['customer_id'] = hash_id(customer_mail)

                except IndexError:  # can't find customer mail
                    row['customer_id'] = 0

                # ensure proper order
                row = row.filter(self.columns)

                enhanced_bookings = enhanced_bookings.append(row)

        with self.output().open('w') as output_file:
            enhanced_bookings.to_csv(
                output_file,
                index=False,
                header=True,
                quoting=csv.QUOTE_NONNUMERIC)


class ScrapeGomusOrderContains(GomusScraperTask):

    # 60 minutes until the task will timeout
    # set to about 800000 for collecting historic data ≈ 7 Days
    worker_timeout = 3600

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return FetchOrdersHTML(base_url=self.base_url + '/admin/orders/')

    def output(self):
        return luigi.LocalTarget(
            'output/gomus/scraped_order_contains.txt', format=UTF8)

    def run(self):

        order_details = []

        with self.input().open('r') as all_htmls:
            for i, html_path in enumerate(all_htmls):
                html_path = html_path.replace('\n', '')
                with open(html_path,
                          'r',
                          encoding='utf-8') as html_file:
                    res_order = html_file.read()

                tree_order = html.fromstring(res_order)

                tree_details = tree_order.xpath(
                    ('//body/div[2]/div[2]/div[3]/div[2]/div[2]/'
                     'div/div[2]/div/div/div/div[2]'))[0]

                # every other td contains the information of an article in the
                # order
                for article in tree_details.xpath(
                        # 'table/tbody[1]/tr[position() mod 2 = 1]'):
                        'table/tbody[1]/tr'):

                    new_article = dict()

                    # Workaround for orders like 671144
                    id_xpath = 'td[1]/div|td[1]/a/div|td[1]/a'
                    if len(article.xpath(id_xpath)) == 0:
                        continue

                    # excursions have a link there and sometimes no div
                    new_article["article_id"] = int(
                        self.extract_from_html(
                            article, id_xpath).strip())

                    order_id = int(re.findall(r'(\d+)\.html$', html_path)[0])
                    new_article["order_id"] = order_id

                    new_article["ticket"] = self.extract_from_html(
                        article, 'td[3]/strong').strip()

                    if new_article["ticket"] == '':
                        continue

                    infobox_str = html.tostring(
                        article.xpath('td[2]/div')[0],
                        method='text',
                        encoding="unicode")

                    # Workaround for orders like 679577
                    raw_date_re = re.findall(r'\d.*Uhr', infobox_str)
                    if not len(raw_date_re) == 0:
                        raw_date = raw_date_re[0]
                    else:
                        # we need something to mark an
                        # invalid / nonexistent date
                        raw_date = '1.1.1900'
                    new_article["date"] = dateparser.parse(raw_date)

                    new_article["quantity"] = int(
                        self.extract_from_html(article, 'td[4]'))

                    raw_price = self.extract_from_html(article, 'td[5]')
                    new_article["price"] = float(
                        raw_price.replace(
                            ",", ".").replace(
                            "€", ""))

                    order_details.append(new_article)

        df = pd.DataFrame(order_details)

        df = self.ensure_foreign_keys(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

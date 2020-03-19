import csv
import os
import re
import time

import dateparser
import luigi
import pandas as pd
import psycopg2
import requests
from luigi.format import UTF8
from lxml import html

from gomus.orders import ExtractOrderData
from gomus._utils.extract_bookings import ExtractGomusBookings
from set_db_connection_options import set_db_connection_options


class FetchGomusHTML(luigi.Task):
    url = luigi.parameter.Parameter(description="The URL to fetch")

    def output(self):
        name = self.url. \
            replace('http://', ''). \
            replace('https://', ''). \
            replace('/', '_') + \
            '.html'

        return luigi.LocalTarget(name, format=UTF8)

    # simply wait for a moment before requesting, as we don't want to
    # overwhelm the server with our interest in classified information...
    def run(self):
        time.sleep(0.5)
        response = requests.get(
            self.url,
            cookies=dict(
                _session_id=os.environ['GOMUS_SESS_ID']))
        response.raise_for_status()

        with self.output().open('r') as html_out:
            html_out.write(response.text)


# inherit from this if you want to scrape gomus (it might be wise to have
# a more general scraper class if we need to scrape something other than
# gomus)
class GomusScraperTask(luigi.Task):
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


class EnhanceBookingsWithScraper(GomusScraperTask):

    # could take up to an hour to scrape all bookings in the next year
    worker_timeout = 3600 * 24
    columns = luigi.parameter.ListParameter(description="Column names")

    def requires(self):
        return ExtractGomusBookings()

    def output(self):
        return luigi.LocalTarget('output/gomus/bookings.csv', format=UTF8)

    def run(self):
        bookings = pd.read_csv(self.input().path)
        bookings['order_date'] = None
        bookings['language'] = ""
        row_count = len(bookings.index)

        db_booking_rows = []

        enhanced_bookings = pd.DataFrame(columns=self.columns)

        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )

            cur = conn.cursor()

            cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables"
                        f" WHERE table_name=\'gomus_booking\')")
            db_booking_rows = []

            if cur.fetchone()[0]:
                query = (f'SELECT booking_id FROM gomus_booking')
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

                booking_url = (self.base_url + "/admin/bookings/"
                               + str(booking_id))
                print(
                    (f"requesting booking details for id: "
                     f"{str(booking_id)} ({i+1}/{row_count})"))

                html_target = yield FetchGomusHTML(booking_url)
                with html_target.open('r') as html_in:
                    res_details = html_in.read()

                tree_details = html.fromstring(res_details)

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

                enhanced_bookings = enhanced_bookings.append(row)

        with self.output().open('w') as output_file:
            enhanced_bookings.to_csv(
                output_file,
                index=False,
                header=True,
                quoting=csv.QUOTE_NONNUMERIC)


class ScrapeGomusOrderContains(GomusScraperTask):

    worker_timeout = 2000  # seconds ≈ 30 minutes until the task will timeout

    def get_order_ids(self):
        orders = pd.read_csv(self.input().path)
        return orders['order_id']

    def requires(self):
        return ExtractOrderData(
            columns=[
                'order_id',
                'order_date',
                'customer_id',
                'valid',
                'paid',
                'origin'])
        # this array is kind of unnecessary, but currently
        # required by ExtractOrderData()
        # the design of that task requiring a column-array is also
        # questionable, so this line may change later on

    def output(self):
        return luigi.LocalTarget(
            'output/gomus/scraped_order_contains.csv', format=UTF8)

    def run(self):
        order_ids = self.get_order_ids()

        order_details = []

        for i in range(len(order_ids)):
            url = self.base_url + "/admin/orders/" + str(order_ids[i])
            print(
                (f"requesting order details for id: "
                 f"{order_ids[i]} ({i+1} out of {len(order_ids)})"))

            html_target = yield FetchGomusHTML(url)
            with html_target.open('r') as html_in:
                res_order = html_in.read()

            tree_order = html.fromstring(res_order)

            tree_details = tree_order.xpath(
                ('//body/div[2]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/'
                 'div/div/div[2]'))[0]

            # every other td contains the information of an article in the
            # order
            for article in tree_details.xpath(
                    'table/tbody[1]/tr[position() mod 2 = 1]'):

                new_article = dict()

                # Workaround for orders like 671144
                id_xpath = 'td[1]/div|td[1]/a/div|td[1]/a'
                if len(article.xpath(id_xpath)) == 0:
                    continue

                # excursions have a link there and sometimes no div
                new_article["article_id"] = int(
                    self.extract_from_html(
                        article, id_xpath).strip())

                new_article["order_id"] = order_ids[i]

                new_article["ticket"] = self.extract_from_html(
                    article, 'td[3]/strong').strip()

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
        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

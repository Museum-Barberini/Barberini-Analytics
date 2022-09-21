import csv
import datetime as dt
import re

import dateparser
import luigi
import pandas as pd
from luigi.format import UTF8
from lxml import html

from _utils import DataPreparationTask, logger
from ..customers import GomusToCustomerMappingToDb
from .extract_bookings import ExtractGomusBookings
from .extract_customers import hash_id
from .fetch_htmls import FetchBookingsHTML, FetchGomusHTML, FetchOrdersHTML


# inherit from this if you want to scrape gomus (it might be wise to have
# a more general scraper class if we need to scrape something other than
# gomus)
# NOTE: If any xpath expressions should ever break, try to rewrite them using
# attribute filters, e.g. by div class or ID. Don't forget that the DOM looks
# different in a browser compared to what we get via the requests library!
class GomusScraperTask(DataPreparationTask):

    def extract_from_html(self, base_html, xpath):
        # try:
        return html.tostring(base_html.xpath(
            xpath)[0], method='text', encoding="unicode")
        # except IndexError:
        #    return ""

    def parse_text(self, document, xpath='.'):

        elements = document.xpath(xpath)
        assert len(elements) == 1
        element = elements[0]
        text = element if isinstance(element, str) else element.text_content()
        return text.strip()

    def parse_int(self, document, xpath='.'):

        return int(self.parse_text(document, xpath))

    def parse_date(
            self, document, xpath='.', relative_base=None) -> dt.datetime:

        settings = {}
        if relative_base is not None:
            settings['RELATIVE_BASE'] = relative_base

        return dateparser.parse(
            self.parse_text(document, xpath),
            locales=['de'],
            settings=settings
        )


class EnhanceBookingsWithScraper(GomusScraperTask):
    columns = luigi.parameter.ListParameter(description="Column names")
    timespan = luigi.parameter.Parameter(default='_nextYear')

    # could take up to an hour to scrape all bookings in the next year
    worker_timeout = 3600

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        yield ExtractGomusBookings(
            timespan=self.timespan,
            columns=self.columns)
        yield FetchBookingsHTML(
            timespan=self.timespan,
            columns=self.columns)
        # table required for fetch_updated_mail()
        yield GomusToCustomerMappingToDb()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/bookings.csv', format=UTF8)

    def run(self):
        with self.input()[0].open('r') as input_file:
            bookings = pd.read_csv(input_file)

        if self.minimal_mode:
            bookings = bookings.head(5)

        bookings.insert(1, 'customer_id', 0)  # new column at second position
        bookings.insert(len(bookings.columns), 'order_date', None)
        bookings.insert(len(bookings.columns), 'language', '')

        with self.input()[1].open('r') as all_htmls:
            for i, html_path in enumerate(all_htmls):
                html_path = html_path.replace('\n', '')
                with open(html_path,
                          'r',
                          encoding='utf-8') as html_file:
                    res_details = html_file.read()
                tree_details = html.fromstring(res_details)

                booking_details = tree_details.xpath(
                    '//body/div[2]/div[2]/div[3]'
                    '/div[4]/div[2]/div[1]/div[3]')[0]

                # Order Date
                bookings.loc[i, 'order_date'] = self.parse_date(
                    booking_details, 'div[1]/div[2]/small/dl[1]/dd[2]')

                # Language
                bookings.loc[i, 'language'] = self.parse_text(
                    booking_details,
                    '''
                    div/div[1]/dl[2]/dd[
                        contains(preceding-sibling::dt[1]/text(), 'Sprache')
                    ]'''
                ).strip()

                try:
                    customer_details = tree_details.xpath(
                        '/html/body/div[2]/div[2]/div[3]/'
                        'div[4]/div[2]/div[2]/div[2]')[0]

                    # Customer E-Mail (not necessarily same as in report)
                    customer_mail = self.extract_from_html(
                        customer_details,
                        'div[1]/div[1]/div[2]/small[1]').strip().split('\n')[0]

                    if re.match(r'^\S+@\S+\.\S+$', customer_mail):
                        bookings.loc[i, 'customer_id'] = hash_id(customer_mail)

                except IndexError:  # can't find customer mail
                    bookings.loc[i, 'customer_id'] = 0

        all_invalid_bookings = None

        def handle_invalid_bookings(invalid_bookings, _):
            nonlocal all_invalid_bookings
            if all_invalid_bookings is None:
                all_invalid_bookings = invalid_bookings.copy()
            else:
                all_invalid_bookings.append(invalid_bookings)

        bookings = self.filter_fkey_violations(
            bookings, handle_invalid_bookings)
        # fetch invalid E-Mail addresses anew
        if all_invalid_bookings is not None:
            for invalid_booking_id in all_invalid_bookings['booking_id']:
                # Delegate dynamic dependencies in sub-method
                new_mail = self.fetch_updated_mail(invalid_booking_id)
                for yielded_task in new_mail:
                    yield yielded_task

        with self.output().open('w') as output_file:
            bookings.to_csv(
                output_file,
                index=False,
                header=True,
                quoting=csv.QUOTE_NONNUMERIC)

    def fetch_updated_mail(self, booking_id):
        # This would be cleaner to put into an extra function,
        # but dynamic dependencies only work when yielded from 'run()'
        logger.info(f"Fetching new mail for booking {booking_id}")

        # First step: Get customer of booking (cannot use customer_id,
        # since it has been derived from the wrong e-mail address)
        booking_html_task = FetchGomusHTML(
            url=f'/admin/bookings/{booking_id}')
        yield booking_html_task
        with booking_html_task.output().open('r') as booking_html_fp:
            booking_html = html.fromstring(booking_html_fp.read())
        booking_customer = booking_html.xpath(
            '//body/div[2]/div[2]/div[3]/div[4]/div[2]'
            '/div[2]/div[2]/div[1]/div[1]/div[1]/a')[0]
        gomus_id = int(booking_customer.get('href').split('/')[-1])

        # Second step: Get current e-mail address for customer
        customer_html_task = FetchGomusHTML(
            url=f'/admin/customers/{gomus_id}')
        yield customer_html_task
        with customer_html_task.output().open('r') as customer_html_fp:
            customer_html = html.fromstring(customer_html_fp.read())
        customer_email = self.parse_text(
            customer_html,
            '//body/div[2]/div[2]/div[3]/div/div[2]/div[1]'
            '/div/div[3]/div/div[1]/div[1]/div/dl/dd[1]')

        # Update customer ID in gomus_customer
        # and gomus_to_customer_mapping
        customer_id = hash_id(customer_email)
        old_customer = self.db_connector.query(
            query=f'''
                SELECT customer_id
                FROM gomus_to_customer_mapping
                WHERE gomus_id = {gomus_id}
            ''',  # nosec B608
                  # we trust gomus HTML/IDs
            only_first=True)
        if not old_customer:
            logger.warning(
                "Cannot update email address of customer which is not in "
                "database.\nSkipping ...")
            return
        old_customer_id = old_customer[0]

        logger.info(f"Replacing old customer ID {old_customer_id} "
                    f"with new customer ID {customer_id}")

        # References are updated through foreign key
        # references via ON UPDATE CASCADE
        self.db_connector.execute(f'''
            UPDATE gomus_customer
            SET customer_id = {customer_id}
            WHERE customer_id = {old_customer_id}
        ''')  # nosec B608


class ScrapeGomusOrderContains(GomusScraperTask):

    # 60 minutes until the task will timeout
    # set to about 800000 for collecting historic data ≈ 7 Days
    worker_timeout = 3600

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return FetchOrdersHTML()

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/scraped_order_contains.csv',
            format=UTF8
        )

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
                    '//body/div[2]/div[2]/div[3]/div[3]/div[2]/div/div[2]/'
                    'div/div/div/div[2]')[0]

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

                    new_article['article_type'] = str(
                        article.xpath(
                            'td[1]/div/i/@title|td[1]/a/div/'
                            'i/@title|td[1]/a/i/@title'
                        )[0])

                    order_id = int(re.findall(r'(\d+)\.html$', html_path)[0])
                    new_article['order_id'] = order_id

                    # Workaround for orders like 478531
                    # if td[3] has no child, we have nowhere to find the ticket
                    if len(article.xpath('td[3][count(*)>0]')) == 0:
                        continue
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

                    storno_mention = re.findall(
                        r'(S|s)torn(o|ier)',
                        html.tostring(  # mostly matches with "Stornogebühr"
                            article,
                            method='text',
                            encoding='unicode')
                    )
                    new_article['is_cancelled'] = len(storno_mention) > 0

                    order_details.append(new_article)

        df = pd.DataFrame(
            order_details,
            columns=[
                'article_id',
                'article_type',
                'order_id',
                'ticket',
                'date',
                'quantity',
                'price',
                'is_cancelled'
            ])
        df = self.filter_fkey_violations(df)

        with self.output().open('w') as output_file:
            df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

#!/usr/bin/env python3
import os
import time
from urllib import parse

import luigi
import requests
from bs4 import BeautifulSoup

from data_preparation import OUTPUT_DIR
from gomus._utils.fetch_report_helper import REPORT_IDS

# These lists map directly to various Gomus attributes used for editing
# the respective reports
ORDERS_FIELDS = [
    'id',
    'created_at',
    'customer_id',
    'customer_fullname',
    'total_price',
    'total_coupon_price',
    'total_still_to_pay_price',
    'is_valid',
    'payment_status',
    'payment_mode',
    'is_canceled',
    'source',
    'cost_centre',
    'invoiced_at',
    'invoices',
    'storno_invoices']
ORDER_SOURCES = ['gomus', 'onlineshop', 'cashpoint', 'resellershop',
                 'resellerapi', 'widget', 'import']
ORDERS_PAYMENT_MODES = list(range(1, 6))
ORDERS_PAYMENT_STATUSES = [0, 10, 15, 20, 30, 40]

CUSTOMER_LEVELS = [0, 5, 10, 20, 30]

BASE_URL = 'https://barberini.gomus.de'
REPORT_PARAMS = 'report[report_params]'

UTF8 = '✓'
METHOD = 'put'
INFORM_USER = 0

# This task can be used to edit the type of gomus report that
# can be edited via forms
# (i.e. Order Reports, Customer Reports, Entry Reports)


class EditGomusReport(luigi.Task):
    report = luigi.parameter.IntParameter(description="Report ID to edit")
    start_at = luigi.parameter.DateParameter(description="Start date to set")
    end_at = luigi.parameter.DateParameter(description="End date to set")
    unique_entries = luigi.parameter.BoolParameter(
        description="Only used for entries. Describes whether a visting"
                    "customer can only be counted once (unique entry)")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_complete = False
        self.start_at = self.start_at.strftime("%Y-%m-%d")
        self.end_at = self.end_at.strftime("%Y-%m-%d")

        self.csrf_token = self.get_csrf()
        self.__body = f'utf8={UTF8}'

    # obsoletes output() and requires()
    def output(self):
        return luigi.LocalTarget(
            f'{OUTPUT_DIR}/gomus/edit_report_{self.task_id}')

    def run(self):
        self.add_to_body(f'_method={METHOD}')
        self.add_to_body(f'authenticity_token={self.csrf_token}')

        report_type = self.get_report_type()
        if report_type == 'Orders':
            self.add_to_body(f'report[report_type]=Exports::{report_type}')

            self.insert_based(f'{REPORT_PARAMS}[fields][]=', ORDERS_FIELDS)
            self.add_to_body(f'{REPORT_PARAMS}[group]=')
            self.insert_export_dates()
            self.insert_based(
                f'{REPORT_PARAMS}[filter[order_source]][]=',
                ORDER_SOURCES)
            self.insert_based(
                f'{REPORT_PARAMS}[filter[payment_mode]][]=',
                ORDERS_PAYMENT_MODES)
            self.insert_based(
                f'{REPORT_PARAMS}[filter[payment_status]][]=',
                ORDERS_PAYMENT_STATUSES)

        elif (report_type == 'Customers' or report_type == 'Entries'):
            self.add_to_body(f'report[report_type]=Reports::{report_type}')

            self.add_to_body(f'{REPORT_PARAMS}[start_at]={self.start_at} 0:00')
            self.add_to_body(f'{REPORT_PARAMS}[end_at]={self.end_at} 24:00')

            if report_type == 'Customers':
                self.insert_based(
                    f'{REPORT_PARAMS}[customer_level][]=',
                    CUSTOMER_LEVELS,
                    add_base=False)

                only_with_annual_ticket = 0
                uniq_by_email = 0
                self.add_to_body(
                    (f'{REPORT_PARAMS}[only_with_annual_ticket]='
                     f'{only_with_annual_ticket}'))
                self.add_to_body(
                    f'{REPORT_PARAMS}[uniq_by_email]={uniq_by_email}')

            else:
                only_unique_visitors = 0

                self.add_to_body(
                    (f'{REPORT_PARAMS}[only_unique_visitors]='
                     f'{only_unique_visitors}'))

                if self.unique_entries:
                    only_unique_visitors = 1
                    self.add_to_body(
                        (f'{REPORT_PARAMS}[only_unique_visitors]='
                         f'{only_unique_visitors}'))
        else:
            raise NotImplementedError("Unimplemented report type")

        self.add_to_body(f'report[inform_user]={INFORM_USER}')

        parse.quote_plus(self.__body, safe='=')
        response = self.post()
        self.wait_for_gomus()

        with self.output().open('w') as output_file:
            output_file.write(f'{response}')

    def get_csrf(self):
        response = self.get(BASE_URL)
        soup = BeautifulSoup(response.text, features='lxml')
        metas = soup.find_all('meta')
        for meta in metas:
            if meta.has_attr('name') and meta['name'] == 'csrf-token':
                return meta['content']

    def get_report_type(self):
        return {v: k for k, v in REPORT_IDS.items()}[self.report].\
            split('_')[0].capitalize()

    def insert_based(self, base, values, add_base=True):
        if add_base:
            self.add_to_body(base)
        for value in values:
            self.add_to_body(base + str(value))

    def insert_export_dates(self):
        base = f'{REPORT_PARAMS}[filter]'
        modes = [
            'created_between',
            'updated_between',
            'reserved_until_between',
            'canceled_between']
        for mode in modes[:2]:
            self.add_to_body(f'{base}[{mode}][start_at]={self.start_at}')
            self.add_to_body(f'{base}[{mode}][end_at]={self.end_at}')

        for mode in modes[2:]:
            self.add_to_body(f'{base}[{mode}][start_at]=')
            self.add_to_body(f'{base}[{mode}][end_at]=')

    def get(self, url):
        return requests.get(url, cookies=dict(
            _session_id=os.environ['GOMUS_SESS_ID']))

    def post(self):
        return requests.post(
            f'{BASE_URL}/admin/reports/{self.report}',
            self.__body.encode(
                encoding='utf-8'),
            cookies=dict(
                _session_id=os.environ['GOMUS_SESS_ID']),
            headers={
                'X-CSRF-Token': self.csrf_token})

    def wait_for_gomus(self):
        # idea: ensure report is fully refreshed by polling every 5 seconds
        # until response is ok
        res = self.get(f'{BASE_URL}/admin/reports/{self.report}')
        while not res.ok or "Bitte warten" in res.text:
            time.sleep(5)
            res = self.get(f'{BASE_URL}/admin/reports/{self.report}')

    def add_to_body(self, string):
        self.__body += f'&{string}'

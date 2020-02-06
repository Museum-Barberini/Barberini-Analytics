#!/usr/bin/env python3
import luigi
import os
import requests

from bs4 import BeautifulSoup
from fetch_gomus import report_ids
from urllib import parse

ORDERS_FIELDS = ['id', 'created_at', 'customer_id', 'customer_fullname',
    'total_price', 'total_coupon_price', 'total_still_to_pay_price', 'is_valid',
    'payment_status', 'payment_mode', 'is_canceled', 'source', 'cost_centre',
    'invoiced_at', 'invoices', 'storno_invoices']

class EditGomusReport(luigi.Task):
    report = luigi.parameter.IntParameter(description="Report ID to edit")
    start_at = luigi.parameter.DateParameter(description="Start date to set")
    end_at = luigi.parameter.DateParameter(description="End date to set")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders_payment_modes = [i for i in range(1, 6)]
        self.orders_payment_statuses = [0, 10, 15, 20, 30, 40]

        self.order_sources = ['gomus', 'onlineshop', 'cashpoint', 'resellershop',
            'resellerapi', 'widget', 'import']

        self.id_reports = dict([(v, k) for k, v in report_ids.items()])
        self.base_url = 'https://barberini.gomus.de/'

        self.utf8 = '✓'
        self.method = 'put'
        self.csrf_token = self.get_csrf()
        self.inform_user = 0

        self.__body = f'utf8={self.utf8}'

    def add_body(self, string):
        self.__body += f'&{string}'

    # obsoletes output() and requires()
    def complete(self):
        return False

    def run(self):
        self.add_body(f'_method={self.method}')
        self.add_body(f'authenticity_token={self.csrf_token}')
        self.add_body(f'report[report_type]={self.get_report_type()}')
        
        self.insert_based('report[report_params][fields][]=', ORDERS_FIELDS)
        self.add_body('report[report_params][group]=')
        self.insert_dates()
        self.insert_based('report[report_params][filter[order_source]][]=', self.order_sources)
        self.insert_based('report[report_params][filter[payment_mode]][]=', self.orders_payment_modes)
        self.insert_based('report[report_params][filter[payment_status]][]=', self.orders_payment_statuses)
        self.add_body(f'report[inform_user]={self.inform_user}')

        parse.quote(self.__body, safe='=')
        self.post()

        # idea: ensure report is fully refreshed by polling every 5 seconds until 200?

    def get_csrf(self):
        response = self.get(self.base_url)
        soup = BeautifulSoup(response.text, features='lxml')
        metas = soup.find_all('meta')
        for meta in metas:
            if meta.has_attr('name') and meta['name'] == 'csrf-token':
                return meta['content']

    def get_report_type(self):
        return 'Exports::' + self.id_reports[self.report].split('_')[0].capitalize()
    
    def insert_based(self, base, values):
        self.add_body(base)
        for value in values:
            self.add_body(base + str(value))

    def insert_dates(self):
        base = 'report[report_params][filter]'
        modes = ['created_between', 'updated_between', 'reserved_until_between', 'canceled_between']
        unwanted_modes = modes[2:]
        for mode in modes[:2]:
            self.add_body(f'{base}[{mode}][start_at]={self.start_at.strftime("%Y-%m-%d")}')
            self.add_body(f'{base}[{mode}][end_at]={self.end_at.strftime("%Y-%m-%d")}')

        for mode in modes[2:]:
            self.add_body(f'{base}[{mode}][start_at]=')
            self.add_body(f'{base}[{mode}][end_at]=')

    def get(self, url):
        return requests.get(url, cookies=dict(_session_id=os.environ['GOMUS_SESS_ID']))

    def post(self):
        return requests.post(f'https://barberini.gomus.de/admin/reports/{self.report}',
            self.__body.encode(encoding='utf-8'), cookies=dict(_session_id=
            os.environ['GOMUS_SESS_ID']), headers={'X-CSRF-Token': self.csrf_token})

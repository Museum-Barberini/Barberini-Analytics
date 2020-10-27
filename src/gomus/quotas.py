"""Provides tasks for downloading gomus quotas into the database."""

import requests

import dateparser
import luigi
import luigi.format
from luigi.format import UTF8
from lxml import html
import pandas as pd
from tqdm.auto import tqdm

from _utils import CsvToDb, DataPreparationTask, logger
from ._utils.fetch_htmls import FetchGomusHTML


class QuotasToDb(CsvToDb):

    table = 'gomus_quota'

    def requires(self):

        return ExtractQuotas()


class ExtractQuotas(DataPreparationTask):

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/quotas.csv',
            format=UTF8
        )

    def requires(self):

        return FetchQuotas()

    def run(self):

        with self.input().open() as input_:
            df_htmls = pd.read_csv(input_)

        tqdm.pandas(desc="Extracting quota")
        quotas = df_htmls['file_path'].progress_apply(self.extract_quota)

        df_quotas = pd.DataFrame(list(quotas))
        with self.output().open('w') as output:
            df_quotas.to_csv(output, index=False)

    def extract_quota(self, html_path):

        with open(html_path) as file:
            dom = html.fromstring(file.read())
            dom: html.HtmlElement

        div = dom.xpath('//body/div[2]/div[2]/div[3]/div/div[2]/div[1]')[0]
        date_div = div.xpath('div[3]/div/div[2]/div/small/dl')[0]

        return dict(
            quota_id=int(self.parse_text(div.xpath(
                '//body/div[2]/div[2]/div[2]/div/div/ol/li[2]/span[1]')[0])),
            name=self.parse_text(div.xpath('div[2]/h3')[0]),
            creation_date=self.parse_date(date_div.xpath('dd[2]')[0]),
            update_date=self.parse_date(date_div.xpath('dd[1]')[0])
        )

    def parse_text(self, html_element):

        return html_element.text_content().strip()

    def parse_date(self, html_element):

        return dateparser.parse(
            self.parse_text(html_element),
            locales=['de']
        )


class FetchQuotas(DataPreparationTask):

    max_missing_ids = 20

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/quota_files.csv',
            format=UTF8
        )

    def run(self):

        quota_id = last_confirmed_id = 0
        with self.output().open('w') as output:
            print('file_path', file=output)

            while quota_id - last_confirmed_id < self.max_missing_ids:
                quota_id += 1
                html = yield FetchGomusHTML(
                    url=f'https://barberini.gomus.de/admin/quotas/'
                        f'{quota_id}',
                    raise_for_status=False)
                if html.has_error():
                    logger.debug(f"Skipping invalid quota_id={quota_id}")
                    continue
                last_confirmed_id = quota_id
                print(html.path, file=output)


class FetchQuotaHTML(FetchGomusHTML):

    quota_id = luigi.IntParameter()

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/quota_{self.quota_id}.html',
            format=luigi.format.Nop
        )

    def run(self):

        response = requests.get(self.url, stream=True)
        response.raise_for_status()

        with self.output().open('wb') as output:
            for block in response.iter_content(1024):
                output.write(block)

    @property
    def url(self):

        return f'https://barberini.gomus.de/admin/quotas/{self.quota_id}'

"""Provides tasks for downloading gomus quotas into the database."""

import luigi
from luigi.format import UTF8
from lxml import html
import pandas as pd
from tqdm import tqdm

from _utils import CsvToDb, DataPreparationTask, logger
from ._utils.fetch_htmls import FetchGomusHTML
from ._utils.scrape_gomus import GomusScraperTask


class QuotasToDb(CsvToDb):
    """Store extract quotas into the database."""

    table = 'gomus_quota'

    def requires(self):

        return ExtractQuotas()


class ExtractQuotas(GomusScraperTask):
    """Extract quotas from the the fetched gomus pages."""

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

        tqdm.pandas(desc="Extracting quotas")
        quotas = df_htmls['file_path'].progress_apply(self.extract_quota)

        df_quotas = pd.DataFrame(list(quotas))
        with self.output().open('w') as output:
            df_quotas.to_csv(output, index=False)

    def extract_quota(self, html_path):

        with open(html_path) as file:
            dom: html.HtmlElement = html.fromstring(file.read())

        div = dom.xpath('//body/div[2]/div[2]/div[3]/div/div[2]/div[1]')[0]
        date_div = div.xpath('div[3]/div/div[2]/div/small/dl')[0]

        return dict(
            quota_id=self.parse_int(
                dom,
                '//body/div[2]/div[2]/div[2]/div/div/ol/li[2]/span[1]'),
            name=self.parse_text(div, 'div[2]/h3'),
            creation_date=self.parse_date(date_div, 'dd[2]'),
            update_date=self.parse_date(date_div, 'dd[1]')
        )


class FetchQuotas(DataPreparationTask):
    """Fetch all quota pages from the gomus site."""

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

            while quota_id - last_confirmed_id <= self.max_missing_ids:
                quota_id += 1
                if self.minimal_mode:
                    quota_id += 5 - 1

                html = yield FetchGomusHTML(
                    url=f'/admin/quotas/'f'{quota_id}',
                    ignored_status_codes=[404])
                if html.has_error():
                    logger.debug(f"Skipping invalid quota_id={quota_id}")
                    continue
                last_confirmed_id = quota_id
                print(html.path, file=output)

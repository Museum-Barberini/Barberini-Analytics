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
    """Store extracted quotas into the database."""

    table = 'gomus_quota'

    replace_content = True

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

        df_quotas = pd.DataFrame(list(quotas), columns=[
            'quota_id', 'name', 'creation_date', 'update_date'])
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

    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/quota_files.csv',
            format=UTF8
        )

    def requires(self):
        return FetchQuotaIds()

    def run(self):
        with self.input().open() as f:
            quota_ids = pd.read_csv(f)['quota_id']

        if self.minimal_mode:
            quota_ids = quota_ids[:5]

        quota_files = []
        with self.output().open('w') as output:
            for quota_id in quota_ids:
                html_target = yield FetchGomusHTML(
                    url=f'/admin/quotas/'f'{quota_id}',
                    ignored_status_codes=[404])
                if html_target.has_error():
                    logger.error(f"Skipping invalid quota_id={quota_id}")
                    continue
                quota_files.append(html_target.path)

        df = pd.DataFrame(quota_files, columns=['file_path'])
        with self.output().open('w') as output:
            df.to_csv(output, index=False)


class FetchQuotaIds(DataPreparationTask):
    def output(self):
        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/quota_ids.csv',
            format=UTF8
        )

    url = '/admin/quotas?per_page=100'

    def run(self):
        quota_ids = []
        url = self.url
        page = 0
        while True:
            logger.debug(f"Fetching {url}...")
            html_target = yield FetchGomusHTML(url=url)
            with html_target.open() as f:
                dom: html.HtmlElement = html.fromstring(f.read())
            quota_ids.extend([
                int(a.attrib['href'].split('/')[-1])
                for a in dom.xpath(
                    '/html/body/div[2]/div[2]/div[3]/div/div[2]/div/div[2]/'
                    'table/tbody/tr/td[1]/a'
                )
            ])
            next_page = dom.xpath(
                '/html/body/div[2]/div[2]/div[3]/div/div[2]/div/div[2]/div/'
                'div[1]/ul/li/a[@rel="next"]'
            )
            if not next_page:
                break
            url = next_page[0].attrib['href']
            print(url)
            if self.minimal_mode and page > 1:
                break
            page += 1

        df = pd.DataFrame(quota_ids, columns=['quota_id'])
        with self.output().open('w') as output:
            df.to_csv(output, index=False)

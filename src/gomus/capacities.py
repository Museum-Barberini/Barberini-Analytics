"""Provides tasks for downloading gomus capacities into the database."""

import datetime as dt
from typing import Iterable

import js2py
import luigi
from luigi.format import UTF8
from lxml import html
import nptime as npt
import pandas as pd
import regex

from _utils import CsvToDb, DataPreparationTask, QueryDb, logger
from ._utils.fetch_htmls import FetchGomusHTML
from ._utils.scrape_gomus import GomusScraperTask
from .quotas import QuotasToDb

SLOT_LENGTH_MINUTES = 15


class CapacitiesToDb(CsvToDb):
    """Store the fetched gomus capacities into the database."""

    table = 'gomus_capacity'

    def requires(self):

        return ExtractCapacities()


class ExtractCapacities(GomusScraperTask):
    """Extract all capacities from the fetched gomus pages."""

    today = luigi.DateSecondParameter(default=dt.datetime.today())

    worker_timeout = 1800  # 30 minutes (1 it/s)

    ignored_error_messages = [
        "Für dieses Kontingent können keine Kapazitäten berechnet werden."
    ]

    popover_pattern = regex.compile(
        r'''
        <script> \s* \$\("\#info-\d+"\)\.popover\( ( \{ \s*
            (?<elem> \w+ \s* : \s* '(?:\\.|[^\\\'])*' \s*){0}
            (?:(?&elem) , \s*)*
            (?&elem)
        \} ) \); \s* </script>
        ''',
        flags=regex.X
    )

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/capacities.csv',
            format=UTF8
        )

    def requires(self):

        return FetchCapacities(today=self.today)

    def run(self):

        with self.input().open() as input_:
            df_htmls = pd.read_csv(input_)

        capacities = [
            self.extract_capacities(html_path)
            for html_path in self.tqdm(
                df_htmls['file_path'],
                desc="Extracting capacities")
        ]

        df_capacities = pd.DataFrame(columns=[
            'quota_id', 'date', 'time',
            'max', 'sold', 'reserved', 'available', 'last_updated'
        ])
        if capacities:
            df_capacities = pd.concat([df_capacities, *capacities])
            df_capacities['last_updated'] = self.today

        with self.output().open('w') as output:
            df_capacities.to_csv(output, index=False)

    def extract_capacities(self, html_path):

        with open(html_path) as file:
            src = file.read()
        dom: html.HtmlElement = html.fromstring(src)

        self.quota_id, self.min_date = self.extract_header(dom)
        logger.debug(
            "Scraping capacities from quota_id=%s for min_date=%s",
            self.quota_id, self.min_date)

        capacities = self.create_zero_data(self.min_date)

        def load_data(data):
            return pd.DataFrame(
                data,
                columns=[*capacities.index.names, *capacities.columns],
                dtype=object
            ).set_index(capacities.index.names)

        basic_capacities = load_data(self.extract_basic_capacities(dom))
        capacities.update(basic_capacities)

        detailed_capacities = load_data(
            self.extract_detailed_capacities(src, self.min_date))
        capacities.update(detailed_capacities)

        capacities = capacities.reset_index()
        capacities.insert(0, 'quota_id', self.quota_id)

        return capacities

    def create_zero_data(self, min_date: dt.date):

        df = pd.DataFrame(columns=[
            'max', 'sold', 'reserved', 'available'
        ])
        dates = [min_date + dt.timedelta(days=days) for days in range(0, 7)]
        times = list(self.create_time_range(
            delta=dt.timedelta(minutes=SLOT_LENGTH_MINUTES)))
        return df.reindex(
            pd.MultiIndex.from_product(
                [dates, times],
                names=['date', 'time']),
            fill_value=0)

    def extract_header(self, dom: html.HtmlElement):
        """Extract general information from the DOM, e.g. quota ID or date."""
        quota_id = self.parse_int(
            dom,
            '//body/div[2]/div[2]/div[2]/div/div/ol/li[2]/a/div')
        min_date = self.parse_date(
            dom,
            '//body/div[2]/div[2]/div[3]/div/div[1]/div/div[2]/form/div[2]/'
            'div/div/input/@value')
        return quota_id, min_date

    def extract_basic_capacities(self, dom: html.HtmlElement):
        """
        Extract basic capacity values from the DOM.

        These are the values from the table indicating the availabilities for
        each slot. Generally, this is only a subset of data returned by
        extract_detailed_capacities(). However, in some cases, gomus displays
        (defect) negative values in the table and does not provide details
        about them, so this method is required to record the defect values
        anyway.
        """
        cells = dom.xpath(
            '//body/div[2]/div[2]/div[3]/div/div[2]/div/div[2]/table/tbody/'
            'tr/td[position()>1]')
        if not cells:
            all_text = dom.text_content()
            if any(
                message in all_text
                for message in self.ignored_error_messages
            ):
                return
            raise ValueError(f"Failed to extract any basic capacity from DOM "
                             f"for quota_id={self.quota_id}, "
                             f"min_date={self.min_date}!")
        for cell in cells:
            datetime = dt.datetime.fromtimestamp(
                int(cell.get('data-timestamp')))
            available = int(cell.text_content().strip())
            yield dict(
                date=datetime.date(),
                time=datetime.time(),
                max=available,
                available=available
            )

    def extract_detailed_capacities(self, src: str, min_date: dt.date):
        """Extract capacity details from the hovercards in the HTML source."""
        js_infos = [match[0] for match in self.popover_pattern.findall(src)]
        infos = [js2py.eval_js(f'd = {js}') for js in js_infos]
        for info in infos:
            yield self.extract_capacity(info, min_date)

    def extract_capacity(self, info, min_date):
        """Extract capacity details from a single hovercard info."""
        title: html.HtmlElement = html.fromstring(info['title'])
        content: html.HtmlElement = html.fromstring(info['content'])

        datetime = self.parse_date(title, relative_base=min_date)

        return dict(
            date=datetime.date(),
            time=datetime.time(),
            max=self.parse_int(content, '//tbody[1]/tr[1]/td[2]'),
            sold=self.parse_int(content, '//tbody[1]/tr[2]/td[2]'),
            reserved=self.parse_int(content, '//tbody[1]/tr[3]/td[2]'),
            available=self.parse_int(content, '//tfooter[1]/tr/td[2]')
        )

    @staticmethod
    def create_time_range(delta: dt.timedelta) -> Iterable[dt.time]:
        assert delta.days == 0
        time = npt.nptime()
        while True:
            yield time
            prev_time = time
            time += delta
            if time <= prev_time:
                break


class FetchCapacities(DataPreparationTask):
    """
    Fetch the capacity pages for all known quotas from the gomus system.

    As a rule of thumb, with the default parameter configuration, this task
    takes about 1 minute to run per invalidated quota, including downstream
    dependencies.
    """

    weeks_back = luigi.IntParameter(8)
    weeks_ahead = luigi.IntParameter(52)
    today = luigi.DateParameter()

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        if self.minimal_mode:
            if self.weeks_back == type(self).weeks_back._default:
                self.weeks_back = 2
            if self.weeks_ahead == type(self).weeks_ahead._default:
                self.weeks_ahead = 2

    def output(self):

        return luigi.LocalTarget(
            f'{self.output_dir}/gomus/capacity_files.csv',
            format=luigi.format.UTF8)

    def _requires(self):

        return luigi.task.flatten([
            QuotasToDb(),
            super()._requires()])

    def run(self):

        current_date = self.today - dt.timedelta(days=self.today.weekday())
        min_date = current_date - dt.timedelta(weeks=self.weeks_back)
        max_date = current_date + dt.timedelta(weeks=self.weeks_ahead)

        invalid_csv = yield QueryDb(
            query='''
                SELECT DISTINCT
                    gq.quota_id, date_trunc('WEEK', dt.date) date
                FROM gomus_quota gq
                CROSS JOIN (
                    (
                        SELECT date(date)
                        FROM generate_series(
                            %(min_date)s, %(max_date)s, %(datedelta)s) date
                    ) date CROSS JOIN (
                        SELECT CAST(make_interval(mins=>mins) AS time) AS time
                        FROM generate_series(
                            0, 60 * 24 - %(mindelta)s, %(mindelta)s) mins
                    ) time) dt
                LEFT JOIN gomus_capacity gc
                ON (dt.date, dt.time, gq.quota_id) =
                    (gc.date, gc.time, gc.quota_id)
                WHERE last_updated >= update_date IS NOT TRUE
            ''',
            kwargs=dict(
                min_date=min_date, max_date=max_date,
                datedelta=dt.timedelta(days=1),
                mindelta=SLOT_LENGTH_MINUTES))

        with invalid_csv.open() as csv:
            invalid_df = pd.read_csv(csv, parse_dates=['date'])

        with self.output().open('w') as output:
            print('file_path', file=output)
            for quota_id, date in self.tqdm(
                    invalid_df.itertuples(index=False),
                    total=len(invalid_df),
                    desc="Fetching capacities HTML"):
                html = yield FetchGomusHTML(
                    url=f'/admin/quotas/{quota_id}'
                        f'/capacities?start_at={date.date()}')
                print(html.path, file=output)

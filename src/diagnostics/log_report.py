import datetime as dt
import glob
from pathlib import Path
import socket

import luigi
from luigi.notifications import send_error_email
import regex
import pandas as pd
from tqdm import tqdm

from _utils import utils, OUTPUT_DIR


# Log patterns ---

STOP_PATTERN = regex.compile(
    r'''
        ^(
            INFO:\s*Informed\ scheduler\ that\ task
            \s+ (?P<task_id>([\w_]+)) \s+
            has\ status \s+ (?P<status>(\w+))
        |
            ERROR:\s*\[pid \s* \d+\]
            \s+ Worker \s+ (?P<worker2>[\w_]+) \(.*\)
            \s* failed \s* (?P<task_name2>(\w+)) (?P<task_params2>\(.*\))
            \s*
        )$
    ''',
    flags=regex.MULTILINE | regex.VERBOSE
)

NOISE_PATTERN = regex.compile(
    rf'''
        (
            (?!{STOP_PATTERN.pattern})
            [\S\s]
        )*?
    ''',
    flags=regex.MULTILINE | regex.VERBOSE
)

PATTERN = regex.compile(
    rf'''
        # 1. Task was started
        ^
        INFO:\s*\[pid \s* \d+\]
        \s+ Worker \s+ (?P<worker>[\w_]+) \(.*\)
        \s* running \s* (?P<task_name>(\w+)) (?P<task_params>\(.*\))
        \s* $

        # 2. Noise
        {NOISE_PATTERN.pattern}

        (
            # 3. Logger error or warning from task
            (?P<log_string>
                ^
                (?P<log_level>(ERROR|WARNING)):
                (?!\s*\[pid .*\])  # Ignore scheduling errors (they already
                                   # were reported)
                .+
                # Match consecutive lines greedily
                (
                    \n(?P=log_level):
                    (?!\s*\[pid .*\]) .*
                )*
            )

            # 4. Noise
            {NOISE_PATTERN.pattern}
        )+

        # 5. Task was stopped
        {STOP_PATTERN.pattern}
    ''',
    flags=regex.MULTILINE | regex.VERBOSE
)

# ---


class SendLogReport(luigi.Task):

    today = luigi.DateParameter(
        default=dt.date.today() - dt.timedelta(days=1)
    )
    days_back = luigi.IntParameter(
        default=7
    )

    def requires(self):

        return CollectLogReport(today=self.today, days_back=self.days_back)

    def run(self):

        with self.input().open() as csv:
            logs = pd.read_csv(csv)

        log_summary = logs.groupby(
            ['task_name', 'log_level']
        )['log_string'].count().to_frame().unstack(
            level='log_level'
        )
        log_summary.columns = log_summary.columns.to_flat_index().map(
            lambda title: title[-1]
        )
        log_summary = log_summary.rename(
            {
                'ERROR': 'error_count',
                'WARNING': 'warning_count'
            },
            axis=1
        ).reset_index()
        for type_ in ['error', 'warning']:
            log_summary[f'{type_}_count'] = \
                log_summary[f'{type_}_count'].fillna(0).astype(int)

        django_renderer = utils.load_django_renderer()
        send_error_email(
            subject=f"Weekly log report ({len(logs)} incidents)",
            message=django_renderer(
                'data/strings/log_report_email.html',
                context=dict(
                    host=socket.gethostname(),
                    logs=logs,
                    log_summary=log_summary
                )))


class CollectLogReport(luigi.Task):

    today = luigi.DateParameter()
    days_back = luigi.IntParameter()

    def output(self):

        return luigi.LocalTarget(f'{OUTPUT_DIR}/log_report.csv')

    def run(self):

        log_dates = [
            self.today - dt.timedelta(days=days)
            for days in range(self.days_back, 0, -1)
        ]
        log_names = {
            date: [
                name
                for name
                in glob.glob(f'/var/log/barberini-analytics/*-{date}.log')
            ]
            for date in log_dates
        }
        log_strings = {
            date: '\n'.join(
                Path(name).read_text()
                for name in names
            )
            for date, names in log_names.items()
        }
        logs = pd.DataFrame(
            dict(
                date=date,
                task_name=match.group('task_name'),
                task_params=match.group('task_params'),
                logs=list(zip(
                    match.captures('log_level'),
                    match.captures('log_string')
                ))
            )
            for date, log_string in tqdm(
                log_strings.items(),
                desc="Scanning logs"
            )
            for match in regex.finditer(PATTERN, log_string)
        )
        logs = logs.explode(column='logs')
        logs['log_level'], logs['log_string'] = zip(*logs['logs'])
        del logs['logs']

        with self.output().open('w') as csv:
            logs.to_csv(csv, index=False)

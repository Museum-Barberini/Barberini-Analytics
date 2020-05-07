import logging
import os
import sys

import luigi

from db_connector import db_connector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'
OUTPUT_DIR = os.environ['OUTPUT_DIR']


class DataPreparationTask(luigi.Task):

    minimal_mode = luigi.parameter.BoolParameter(
        default=minimal_mode,
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

    foreign_keys = luigi.parameter.ListParameter(
        description="The foreign keys to be asserted",
        default=[])

    @property
    def output_dir(self):
        return OUTPUT_DIR

    def ensure_foreign_keys(self, df):
        filtered_df = df
        invalid_values = None

        if df.empty:
            return filtered_df, invalid_values

        for foreign_key in self.foreign_keys:
            key = foreign_key['origin_column']
            old_count = df[key].count()

            results = db_connector.query(f'''
                SELECT {foreign_key['target_column']}
                FROM {foreign_key['target_table']}
            ''')

            # cast values to 'str' uniformly to prevent
            # mismatching due to wrong data types
            foreign_values = [str(row[0]) for row in results]

            if not isinstance(df[key][0], str):
                df[key] = df[key].apply(str)

            # Remove all rows from the df where the value does not
            # match any value from the referenced table
            filtered_df = df[df[key].isin(foreign_values)]

            difference = old_count - filtered_df[key].count()
            if difference:
                # Find out which values were discarded
                # for potential handling
                invalid_values = df[~df[key].isin(foreign_values)]
                logger.warning(f"Deleted {difference} out of {old_count} "
                               f"data sets due to foreign key violation: "
                               f"{foreign_key}")

                # Only print discarded values if running from a TTY
                # to prevent potentially sensitive data to be exposed
                # (e.g. by the CI runner)
                if sys.stdout.isatty():
                    print(f"Following values were invalid:\n{invalid_values}")
                else:
                    print("Values not printed for privacy reasons")

        return filtered_df, invalid_values

    def iter_verbose(self, iterable, msg, size=None):
        if not sys.stdout.isatty():
            yield from iterable
            return
        if size is None:
            size = len(iterable) if hasattr(iterable, '__len__') else None
        format_args = {
            'item': str(None),
            'index': 0
        }
        if size:
            format_args['size'] = size
        print()
        try:
            for index, item in enumerate(iterable, start=1):
                format_args['index'] = index
                format_args['item'] = item
                message = msg.format(**format_args)
                message = f'\r{message} ... '
                if size:
                    message += f'({(index - 1) / size :2.1%})'
                print(message, end=' ', flush=True)
                yield item
            print(f'\r{msg.format(**format_args)} ... done.', flush=True)
        except Exception:
            print(flush=True)
            raise

    def loop_verbose(self, while_fun, item_fun, msg, size=None):
        def loop():
            while while_fun():
                yield item_fun()
        return self.iter_verbose(loop(), msg, size=size)

#!/usr/bin/env python3
"""Condenses redundant performance values stored for posts in the database."""

import pandas as pd
from _utils import db_connector, logger
from _utils.data_preparation import PerformanceValueCondenser

CONNECTOR = db_connector()
PERFORMANCE_TABLES = [
    'tweet_performance',
    'ig_post_performance',
    'fb_post_performance'
]
TIMESTAMP_COLUMN = 'timestamp'


def main():  # noqa: D103

    for table in PERFORMANCE_TABLES:
        condenser = PerformanceValueCondenser(CONNECTOR, table)

        key_columns = condenser.get_key_columns()
        performance_columns = condenser.get_performance_columns(key_columns)
        data, header = CONNECTOR.query_with_header(f'SELECT * FROM {table}')
        df = pd.DataFrame(data, columns=header)

        # Special treatment because of multi-column key
        # (pandas unique only works on series -> 1d)
        if table == 'fb_post_performance':
            df.drop(columns='page_id', inplace=True)
            key_columns.remove('page_id')
        key_column = key_columns[0]

        before = len(df)
        to_drop = []
        unique_ids = df[key_column].unique()

        logger.debug("Condensing performance table: %s", table)
        logger.debug(f"Processing {len(unique_ids)} unique ids")
        logger.debug("Before: %s", before)
        for unique_id in unique_ids:
            ordered_entries = df.loc[df[key_column] == unique_id] \
                .sort_values(by=TIMESTAMP_COLUMN, axis='index', ascending=True)

            prev_row = None
            for i, row in ordered_entries.iterrows():
                if prev_row is None:  # could be 0
                    prev_row = row
                    continue

                # if current and previous entries are equal,
                # flag current entry for deletion
                if row[performance_columns] \
                        .equals(prev_row[performance_columns]):
                    to_drop.append(i)
                prev_row = row

        logger.debug("After: %s", before - len(to_drop))

        to_drop_df = df[df.index.isin(to_drop)]

        # Note this could be optimized by using
        # cursor.copy_from and a temporary table.
        queries = []
        for _, row in to_drop_df.iterrows():
            queries.append(
                f'''
                DELETE FROM {table}
                WHERE {key_column} = '{row[key_column]}'
                AND {TIMESTAMP_COLUMN} = '{row[TIMESTAMP_COLUMN]}'
                '''
            )
        if queries:
            CONNECTOR.execute(*queries)


if __name__ == '__main__':
    main()

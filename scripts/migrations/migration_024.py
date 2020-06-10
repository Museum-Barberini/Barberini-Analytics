#!/usr/bin/env python3
import logging
import pandas as pd
from db_connector import db_connector

logger = logging.getLogger('luigi-interface')

PERFORMANCE_TABLES = [
    'tweet_performance',
    'ig_post_performance',
    'fb_post_performance'
]
TIMESTAMP_COLUMN = 'timestamp'
CONNECTOR = db_connector()


# Utlities copied from DataPreparationTask.condense_performance_values
# ------------------------------------------
def get_key_columns(table):
    primary_key_columns = CONNECTOR.query(
        f'''
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            AS data_type
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{table}'::regclass
        AND    i.indisprimary
        ''')
    return [
        row[0]
        for row in primary_key_columns
        if row[0] != TIMESTAMP_COLUMN]


def get_performance_columns(table, key_columns):
    # This simply returns all columns except the key
    # and timestamp columns of the target table
    db_columns = CONNECTOR.query(
        f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = \'{table}\'
        ''')
    return [
        row[0]
        for row in db_columns
        if row[0] not in key_columns and row[0] != TIMESTAMP_COLUMN]
# ------------------------------------------


for table in PERFORMANCE_TABLES:
    key_columns = get_key_columns(table)
    performance_columns = get_performance_columns(table, key_columns)
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

    logger.debug("Condensing performance table:", table)
    logger.debug(f"Processing {len(unique_ids)} unique ids")
    logger.debug("Before:", before)
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

    logger.debug("After:", before - len(to_drop))

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

#!/usr/bin/env python3
import pandas as pd
from db_connector import db_connector

PERFORMANCE_TABLES = [
    'ig_post_performance',
    'fb_post_performance',
    'tweet_performance'
]
TIMESTAMP_COLUMN = 'timestamp'
CONNECTOR = db_connector()


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


for table in PERFORMANCE_TABLES:
    key_columns = get_key_columns(table)
    performance_columns = get_performance_columns(table, key_columns)
    all_data = CONNECTOR.query_with_header(f'SELECT * FROM {table}')
    df = pd.DataFrame(data=all_data[0], columns=all_data[1])

    # Special treatment because of multi-column key
    # (pandas unique only works on series -> 1d)
    if table == 'fb_post_performance':
        page_ids = df['page_id']
        df.drop(columns='page_id', inplace=True)
        key_columns.remove('page_id')
    key_column = key_columns[0]

    before = df[key_column].count()
    print("Before:", before)
    to_drop = []
    unique_ids = df[key_column].unique()
    print(f"Processing {len(unique_ids)} unique ids")
    for unique_id in unique_ids:
        ordered_entries = df.loc[df[key_column] == unique_id] \
            .sort_values(by=TIMESTAMP_COLUMN, axis='index', ascending=True)

        prev_row = None
        for i, row in ordered_entries.iterrows():
            if prev_row is None:  # could be 0
                prev_row = row
                continue
            if row[performance_columns] \
               .equals(prev_row[performance_columns]):
                to_drop.append(i)
            prev_row = row
    print("To drop:", len(to_drop))
    to_drop_df = df[df.index.isin(to_drop)]
    print("After:", before - len(to_drop))
    break

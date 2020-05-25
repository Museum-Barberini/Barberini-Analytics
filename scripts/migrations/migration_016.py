#!/usr/bin/env python3
"""
Truncate facebook posts (#213)
We want to do this because in the past, we have collected a number of old
posts that are not necessarily authored by the museum itself. To avoid wrong
is_from_museum attributions, we are going to drop all posts so that posts that
are indeed by the museum will be fetched again automatically (see #184).
"""
import logging
import os
import subprocess as sp

import db_connector
db_connector = db_connector.db_connector()

logger = logging.getLogger('luigi-interface')
logging.basicConfig(level=logging.INFO)


REFERENCING_TABLES = ['fb_post_comment', 'fb_post_performance']

# Are there any existing data to preserve?
if not any(
            db_connector.exists(f'SELECT * FROM {table}')
            for table in REFERENCING_TABLES
        ):
    # Nothing to preserve, get into the fast lane
    logger.info("Truncating fb_post in the fast line")
    db_connector.execute('''
        TRUNCATE TABLE fb_post CASCADE
    ''')
    exit(0)

# Otherwise, to keep existing data from referencing tables, we will need to do
# some SQL acrobatics below.
logger.info("Truncating fb_post in the long line")

try:
    with db_connector._create_connection() as conn:
        with conn.cursor() as cur:

            # 1. Decouple performance table from post table
            logger.info("Dropping constraints")
            for table in REFERENCING_TABLES:
                cur.execute(f'''
                    ALTER TABLE {table}
                    DROP CONSTRAINT {table}_page_id_post_id_fkey
                ''')

            # 2. Truncate post table
            logger.info("Truncating fb_post")
            cur.execute('''
                TRUNCATE TABLE fb_post
            ''')

    # 3. Fetch posts again
    logger.info("Fetching posts again")
    os.environ['OUTPUT_DIR'] = 'output_migration_015'
    sp.run(
        check=True,
        args='''make
            luigi-restart-scheduler
            luigi-clean
            luigi-task LMODULE=facebook LTASK=FbPostsToDB
            luigi-clean
        '''.split()
    )

    with conn:
        with conn.cursor() as cur:

            # 4. Drop invalidated values
            logger.info("Dropping invalid values")
            for table in REFERENCING_TABLES:
                cur.execute(f'''
                    DELETE FROM {table}
                    WHERE (page_id, post_id)
                    NOT IN (select page_id, post_id from fb_post)
                ''')

            # 5. Reconnect related tables
            logger.info("Reconnecting")
            for table in REFERENCING_TABLES:
                cur.execute(f'''
                    ALTER TABLE {table}
                        ADD FOREIGN KEY (page_id, post_id) REFERENCES fb_post
                ''')

finally:
    conn.close()

logger.info("Truncation done")

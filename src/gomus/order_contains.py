#!/usr/bin/env python3
import luigi

from csv_to_db import CsvToDb

from ._utils.scrape_gomus import ScrapeGomusOrderContains


class OrderContainsToDB(CsvToDb):

    table = 'gomus_order_contains'

    columns = [
        ('article_id', 'INT'),
        ('order_id', 'INT'),
        ('ticket', 'TEXT'),
        ('date', 'DATE'),
        ('quantity', 'INT'),
        ('price', 'FLOAT'),
    ]
    
    primary_key = 'article_id'

    foreign_keys = [
            {
                "origin_column": "order_id",
                "target_table": "gomus_order",
                "target_column": "order_id"
            }
        ]

    def requires(self):
        return ScrapeGomusOrderContains()

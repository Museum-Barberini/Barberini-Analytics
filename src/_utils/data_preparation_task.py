import logging
import luigi

from db_connector import DbConnector

logger = logging.getLogger('luigi-interface')


class DataPreparationTask(luigi.Task):
    table = luigi.parameter.Parameter(
        description="The name of the table the data should be prepared for",
        default=None)

    def ensure_foreign_keys(self, df):
        if df.empty:
            return df

        for foreign_key in self.foreign_keys().items():
            column, (foreign_table, foreign_column) = foreign_key
            old_count = df[column].count()

            foreign_values = [value for [value] in DbConnector.query(f'''
                SELECT {foreign_column}
                FROM {foreign_table}
            ''')]

            # Remove all rows from the df where the value does not
            # match any value from the referenced table
            df = df[df[column].isin(foreign_values)]

            difference = old_count - df[column].count()
            if difference:
                logger.warning(
                    f"Skipped {difference} out of {old_count} "
                    f"data sets due to foreign key violation: "
                    f"{foreign_key}")

        return df

    def foreign_keys(self):
        if not self.table:
            return {}

        return {
            column: (foreign_table, foreign_column)
            for [column, foreign_table, foreign_column]
            in DbConnector.query(f'''
                --- CREDITS: https://stackoverflow.com/a/1152321
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name='{self.table}';
            ''')
        }

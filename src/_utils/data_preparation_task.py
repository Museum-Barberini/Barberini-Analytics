import logging
import luigi

from db_connector import DbConnector

logger = logging.getLogger('luigi-interface')


class DataPreparationTask(luigi.Task):
    foreign_keys = luigi.parameter.ListParameter(
        description="The foreign keys to be asserted",
        default=[])

    def ensure_foreign_keys(self, df):
        if df.empty:
            return df

        for foreign_key in self.foreign_keys:
            key = foreign_key['origin_column']
            old_count = df[key].count()

            results = DbConnector.query(f'''
                SELECT {foreign_key['target_column']}
                FROM {foreign_key['target_table']}
            ''')

            foreign_values = [row[0] for row in results]

            # Remove all rows from the df where the value does not
            # match any value from the referenced table
            df = df[df[key].isin(foreign_values)]

            difference = old_count - df[key].count()
            if difference > 0:
                logger.warning(f"Deleted {difference} out of {old_count} "
                               f"data sets due to foreign key violation: "
                               f"{foreign_key}")

        return df

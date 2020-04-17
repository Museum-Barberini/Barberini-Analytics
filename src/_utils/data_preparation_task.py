import logging
import luigi
import os

from db_connector import DbConnector

logger = logging.getLogger('luigi-interface')

minimal_mode = os.getenv('MINIMAL') == 'True'


class DataPreparationTask(luigi.Task):

    minimal_mode = luigi.parameter.BoolParameter(
        default=minimal_mode,
        description="If True, only a minimal amount of data will be prepared"
                    "in order to test the pipeline for structural problems")

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
            if difference:
                logger.warning(f"Deleted {difference} out of {old_count} "
                               f"data sets due to foreign key violation: "
                               f"{foreign_key}")

        return df

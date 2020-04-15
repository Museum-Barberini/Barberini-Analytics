import sys
import logging
import luigi

from db_connector import DbConnector

logger = logging.getLogger('luigi-interface')


class DataPreparationTask(luigi.Task):
    foreign_keys = luigi.parameter.ListParameter(
        description="The foreign keys to be asserted",
        default=[])

    def ensure_foreign_keys(self, df):
        filtered_df = df
        invalid_values = None

        if df.empty:
            return filtered_df, invalid_values

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
            filtered_df = df[df[key].isin(foreign_values)]

            difference = old_count - filtered_df[key].count()
            if difference > 0:
                # Find out which values were discarded
                # for potential handling
                invalid_values = df[~df[key].isin(foreign_values)]
                logger.warning(f"Deleted {difference} out of {old_count} "
                                f"data sets due to foreign key violation: "
                                f"{foreign_key}")
                if sys.stdout.isatty():
                    print(invalid_values)
                else:
                    print("Values not printed for privacy reasons")

        return df

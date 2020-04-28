import logging
import sys

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

            results = DbConnector.query(
                f"SELECT {foreign_key['target_column']} "
                f"FROM {foreign_key['target_table']}"
            )

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

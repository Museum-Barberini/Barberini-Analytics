import sys

import luigi
import psycopg2

from set_db_connection_options import set_db_connection_options


class DataPreparationTask(luigi.Task):
    foreign_keys = luigi.parameter.ListParameter(
        description="The foreign keys to be asserted",
        default=[])

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def ensure_foreign_keys(self, df):
        filtered_df = df
        invalid_values = None

        if df.empty:
            return filtered_df, invalid_values

        try:
            conn = psycopg2.connect(
                host=self.host, database=self.database,
                user=self.user, password=self.password
            )

            for foreign_key in self.foreign_keys:
                key = foreign_key['origin_column']
                old_count = df[key].count()

                cursor = conn.cursor()
                query = (f"SELECT {foreign_key['target_column']} "
                         f"FROM {foreign_key['target_table']}")
                cursor.execute(query)

                foreign_values = [row[0] for row in cursor.fetchall()]

                # Remove all rows from the df where the value does not
                # match any value from the referenced table
                filtered_df = df[df[key].isin(foreign_values)]

                difference = old_count - filtered_df[key].count()
                if difference > 0:
                    # Find out which values were discarded
                    # for potential handling
                    invalid_values = df[~df[key].isin(foreign_values)]
                    print(f"INFO: Deleted {difference} out of {old_count} "
                          f"data sets due to foreign key violation: ")
                    if sys.stdout.isatty():
                        print(invalid_values)
                    else:
                        print("Values not printed for privacy reasons")

            return filtered_df, invalid_values

        finally:
            if conn is not None:
                conn.close()

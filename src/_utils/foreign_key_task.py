import luigi
import psycopg2

from set_db_connection_options import set_db_connection_options


class ForeignKeyTask(luigi.Task):
    foreign_keys = luigi.parameter.ListParameter(
        description="The foreign keys to be asserted")

    host = None
    database = None
    user = None
    password = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        set_db_connection_options(self)

    def _requires(self):
        raise NotImplementedError("Use this method to introduce "
                                  "dependencies to ToDB tasks for "
                                  "foreign key relations")

    def ensure_foreign_keys(self, df):

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
                df = df[df[key].isin(foreign_values)]

                difference = old_count - df[key] \
                    .count()
                if difference > 0:
                    print(f"INFO: Deleted {difference} out of {old_count} "
                        f"data sets due to foreign key violation: "
                        f"{foreign_key}")

            return df

        finally:
            if conn is not None:
                conn.close()

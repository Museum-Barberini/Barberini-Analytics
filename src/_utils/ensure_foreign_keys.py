import psycopg2


def ensure_foreign_keys(df, foreign_keys, host, database, user, password):

    try:
        conn = psycopg2.connect(
            host=host, database=database,
            user=user, password=password
        )

        for foreign_key in foreign_keys:
            old_count = df[foreign_key['origin_column']].count()

            cursor = conn.cursor()
            query = (f"SELECT {foreign_key['target_column']} "
                     f"FROM {foreign_key['target_table']}")
            cursor.execute(query)

            foreign_values = [row[0] for row in cursor.fetchall()]

            # Remove all rows from the df where the value does not
            # match any value from the referenced table
            df = df[df[foreign_key['origin_column']].isin(foreign_values)]

            difference = old_count - df[foreign_key['origin_column']] \
                .count()
            if difference > 0:
                print(f"INFO: Deleted {difference} out of {old_count}"
                      f"data sets due to foreign key violation: "
                      f"{foreign_key}")

        return df

    except psycopg2.DatabaseError:
        raise

    finally:
        if conn is not None:
            conn.close()

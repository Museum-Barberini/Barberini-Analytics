/** arguments:
  * {0}: target schema name
  * {1}: target table name
  * {1}: columns to update, of format foo, comma-separated
  * {2}: column pairs to update, of format foo=EXCLUDED.foo, comma-separated
  */

BEGIN;
CREATE TEMPORARY TABLE {0}_{1}_tmp (LIKE {0}.{1} INCLUDING ALL);
COPY {0}_{1}_tmp FROM stdin WITH (FORMAT CSV);

INSERT INTO {0}.{1} ({2})
    SELECT {2} FROM {0}_{1}_tmp
ON CONFLICT ON CONSTRAINT {1}_pkey
    DO UPDATE SET {3};
COMMIT;

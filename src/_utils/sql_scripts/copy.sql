/** arguments:
  * {0}: target schema name
  * {1}: target table name
  * {2}: primary constraint name
  * {3}: columns to update, of format foo, comma-separated
  * {4}: column pairs to update, of format foo=EXCLUDED.foo, comma-separated
  */

BEGIN;
CREATE TEMPORARY TABLE {0}_{1}_tmp (LIKE {0}.{1} INCLUDING ALL);
COPY {0}_{1}_tmp ({3}) FROM stdin WITH (FORMAT CSV);

INSERT INTO {0}.{1} ({3})
    SELECT {3} FROM {0}_{1}_tmp
ON CONFLICT ON CONSTRAINT {2}
    DO UPDATE SET {4};
COMMIT;

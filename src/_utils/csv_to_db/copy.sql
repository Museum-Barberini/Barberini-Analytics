-- {0} -- target table name
-- {1} -- columns to update

BEGIN;
CREATE TEMPORARY TABLE {0}_tmp (LIKE {0} INCLUDING ALL);
COPY {0}_tmp FROM stdin WITH (FORMAT CSV);

INSERT INTO {0}
	SELECT * FROM {0}_tmp
ON CONFLICT ON CONSTRAINT {0}_the_primary_key_constraint
	DO UPDATE SET {1};
COMMIT;

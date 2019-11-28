BEGIN;
CREATE TEMPORARY TABLE {1} (LIKE {0} INCLUDING ALL);
COPY {1} FROM stdin WITH (FORMAT CSV);
-- If there are many thousands of rows
ANALYZE {1};
INSERT INTO {0}
	SELECT * FROM {1}
	ON CONFLICT DO NOTHING;
COMMIT;

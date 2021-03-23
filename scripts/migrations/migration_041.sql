/*
	Fix #345 (Ambiguous exhibition titles)
	exhibition.short_title contained invalid values because of the stored
	column cache. Since the column computation implicitly depends on
	exhibition_time which again references to exhibition, short_title can only
	be added to the schema in an extra step. To do so, intern the actual
	exhibition table and add the short title via a new exhibition view that
	also keeps the public schema consistent.
*/

BEGIN;

    -- 1. Disconnect referencing views
    CREATE OR REPLACE VIEW exhibition_day AS (
        SELECT *
        FROM exhibition_day
    );

    -- 2. Intern original table and remove stored column
    ALTER TABLE exhibition RENAME TO exhibition_raw;
    ALTER TABLE exhibition_raw DROP COLUMN short_title;

    -- 3. Wrap original table and provide short_title again
    CREATE VIEW exhibition AS (
        SELECT
            *,
            exhibition_short_title(title) AS short_title
        FROM
            exhibition_raw
    );

    -- 4. Reconnect referencing views (NO CHANGE)
    CREATE OR REPLACE VIEW exhibition_day AS (
        SELECT DATE(date), exhibition.title, short_title
        FROM generate_series(
            (SELECT MIN(start_date) FROM exhibition_time),
            now(),
            '1 day'::interval
        ) date
        LEFT JOIN exhibition_time
	    ON date BETWEEN start_date AND end_date
        NATURAL JOIN exhibition
    );

COMMIT;

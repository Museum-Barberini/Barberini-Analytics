/** Revise exhibition schema again
    With the latest changes from !194, it was no longer possible to join
    exhibition with exhibition_day.
    This migration includes the column title into the exhibition_day and
    refactors the view.
  */

BEGIN;

    DROP VIEW exhibition_day;

    CREATE FUNCTION exhibition_short_title(_title text) RETURNS text AS
    $$
        SELECT CONCAT_WS(
            ' ',
            EXTRACT(YEAR FROM MIN(start_date)),
            COALESCE((regexp_match(_title, '.*?\S(?=\s*[\.\/-] )'))[1], _title)
        )
        FROM exhibition_time
            NATURAL JOIN exhibition
        WHERE exhibition_time.title = _title
            AND special IS NULL;
    $$
    LANGUAGE SQL IMMUTABLE;

    ALTER TABLE exhibition
        DROP COLUMN short_title,
        ADD COLUMN short_title TEXT GENERATED ALWAYS AS (
            exhibition_short_title(title)
        ) STORED;

    CREATE VIEW exhibition_day AS (
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

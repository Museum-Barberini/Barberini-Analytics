-- Revise exhibition schema (!183)

BEGIN;

    DROP TABLE exhibition;
    
    CREATE TABLE exhibition (
        title TEXT PRIMARY KEY,
        picture_url TEXT,
        special TEXT  -- NULL (exhibition), 'closing day', or 'presentation'
            GENERATED ALWAYS AS (
                CASE
                    WHEN title = 'Schließtag / Closing Day'
                        THEN 'closing day'
                    WHEN title = 'Präsentationen zwischen den Ausstellungen'
                        THEN 'presentation'
                END
            ) STORED,
        short_title TEXT GENERATED ALWAYS AS (
            coalesce((regexp_match(title, '.*?\S(?=\s*[\.\/-] )'))[1], title)
        ) STORED
    );

    CREATE TABLE exhibition_time (
        title TEXT REFERENCES exhibition,
        start_date DATE,
        end_date DATE,
        PRIMARY KEY (title, start_date, end_date)
    );

    -- This maps every single day to the present exhibition(s)
    CREATE VIEW exhibition_day AS (
        SELECT DATE(date), title
        FROM generate_series(
            (SELECT MIN(start_date) FROM exhibition_time),
            now(),
            '1 day'::interval
        ) date
        LEFT JOIN exhibition_time
	    ON date BETWEEN start_date AND end_date
    );

COMMIT;

BEGIN;

    DROP TABLE exhibition;
    
    CREATE TABLE exhibition(
        title TEXT PRIMARY KEY,
        picture_URL TEXT,
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
            coalesce(((regexp_match(title, '.*?\S(?=\s*[\.\/-] )'))[1]), title)
        ) STORED
    );

    CREATE TABLE exhibition_time(
        title TEXT REFERENCES exhibition,
        start_date DATE,
        end_date DATE,
        PRIMARY KEY (title, start_date, end_date)
    );

COMMIT;

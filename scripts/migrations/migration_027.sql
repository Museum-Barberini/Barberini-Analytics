BEGIN;

    ALTER TABLE absa.post_ngram
        RENAME COLUMN ngram TO phrase;

    CREATE TABLE absa.polarity_sentiws (
        word TEXT,
        pos_tag TEXT,
        weight REAL,
        inflections TEXT[],
        polarity TEXT GENERATED ALWAYS AS (
            CASE
                WHEN weight > 0 THEN 'positive'
                WHEN weight < 0 THEN 'negative'
            END
        ) STORED,
        PRIMARY KEY (word, polarity)
    );

    CREATE TABLE absa.polarity_sepl (
        phrase TEXT PRIMARY KEY,
        weight REAL,
        stddev REAL,
        stderr REAL,
        phrase_type TEXT,
        manual_coorection BOOLEAN
    );

    CREATE VIEW absa.polarity AS
    (
        (
            SELECT
                word AS phrase,
                pos_tag,
                weight,
                polarity,
                'SentiWS' AS dataset
            FROM absa.polarity_sentiws
        ) UNION (
            SELECT
                phrase,
                CASE phrase_type
                    WHEN 'a' THEN 'ADJX'
                    WHEN 'n' THEN 'NN'
                    WHEN 'v' THEN 'VVINF'
                END AS pos_tag,
                weight,
                CASE
                    WHEN weight > 0 THEN 'positive'
                    WHEN weight < 0 THEN 'negative'
                END AS polarity,
                'SePL' AS dataset
            FROM absa.polarity_sepl
        )
    );

COMMIT;

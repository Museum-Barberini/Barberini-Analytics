BEGIN;

    ALTER TABLE absa.post_ngram
        RENAME COLUMN ngram TO phrase;


    CREATE TABLE absa.phrase_polarity_sentiws (
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

    CREATE TABLE absa.phrase_polarity_sepl (
        phrase TEXT PRIMARY KEY,
        weight REAL,
        stddev REAL,
        stderr REAL,
        phrase_type TEXT,
        manual_coorection BOOLEAN
    );

    CREATE VIEW absa.phrase_polarity AS (
        WITH _phrase_polarity AS (
            (
                SELECT
                    word AS phrase,
                    pos_tag,
                    weight,
                    polarity,
                    'SentiWS' AS dataset
                FROM absa.phrase_polarity_sentiws
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
                FROM absa.phrase_polarity_sepl
            )
        )
        SELECT
            *,
            array_length(regexp_split_to_array(phrase, '\s+'), 1) AS n
        FROM
            _phrase_polarity
    );


    CREATE VIEW absa.inflection_sentiws AS (
        SELECT unnest(inflections) inflected, word
        FROM absa.phrase_polarity_sentiws
    );

    CREATE VIEW absa.inflection AS (
        (
            SELECT *, 'SentiWS' AS dataset
            FROM absa.inflection_sentiws
        ) UNION (
            SELECT phrase, phrase, dataset
            FROM absa.phrase_polarity
        )
    );

    CREATE TABLE absa.post_polarity (
        source TEXT,
        post_id TEXT,
        polarity REAL,
        stddev REAL,
        count INT,
        dataset TEXT,
        match_algorithm TEXT,
        PRIMARY KEY (source, post_id, dataset, match_algorithm)
    );

COMMIT;

-- Introduce relations for sentiment baseline (!235)

BEGIN;

    -- 1. Create polarity relations
    CREATE TABLE absa.phrase_polarity_sentiws (
        word TEXT,
        pos_tag TEXT,
        weight REAL,
        CHECK (weight BETWEEN -1 AND 1),
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
        CHECK (weight BETWEEN 1 AND 10),
        stddev REAL,
        stderr REAL,
        phrase_type TEXT,
        manual_correction BOOLEAN
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


    -- 2. Create inflection relations
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


    -- 3. Update schema for post_sentiment
    ALTER TABLE absa.post_ngram
        RENAME COLUMN ngram TO phrase;    

    CREATE TABLE absa.post_sentiment (
        source TEXT,
        post_id TEXT,
        sentiment REAL,
        CHECK (sentiment BETWEEN -1 AND 1),
        stddev REAL,
        subjectivity REAL,
        CHECK (subjectivity BETWEEN -1 AND 1),
        count INT,
        dataset TEXT,
        match_algorithm TEXT,
        PRIMARY KEY (source, post_id, dataset, match_algorithm)
    );

COMMIT;

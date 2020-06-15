BEGIN;

    CREATE TABLE absa.polarity(
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

COMMIT;

-- Add post_sentiment relations (!253)

BEGIN;

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

-- Add simple word and n-gram tables for ABSA (!135)

BEGIN;

    CREATE SCHEMA absa;

    CREATE TABLE absa.stopword (
        word TEXT PRIMARY KEY
    );

    CREATE TABLE absa.post_word (
        source TEXT,
        post_id TEXT,
        word_index INT,
        word TEXT,
        PRIMARY KEY (source, post_id, word_index)
    );

    CREATE TABLE absa.post_ngram (
        source TEXT,
        post_id TEXT,
        n INT,
        word_index INT,
        ngram TEXT,
        PRIMARY KEY (source, post_id, n, word_index)
    );

COMMIT;

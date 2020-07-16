BEGIN;

    CREATE TABLE absa.post_opinion_sentiment (
        source TEXT, post_id TEXT,
        target_aspect_words TEXT[],
        target_aspect_word TEXT GENERATED ALWAYS AS (
            target_aspect_words[1]
        ) STORED,
        count INT NOT NULL CHECK (count > 0),
        sentiment REAL NOT NULL,
        aspect_phrase TEXT,
        dataset TEXT, sentiment_match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, target_aspect_word,
            dataset, sentiment_match_algorithm
        )
    );

COMMIT;

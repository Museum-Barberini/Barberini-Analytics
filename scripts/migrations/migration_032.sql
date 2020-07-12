-- Add post_sentiment relations (!253)

BEGIN;

    ALTER TABLE absa.post_ngram
        RENAME COLUMN ngram TO phrase;

    /** TODO NEXT:
      * [done] post_word_polarity als Tabelle in DB
      * [done] post_aspect_sentiment_linear_distance
      * für distances nur threshold oder auch weight function? weitere variation: weight vs limit
      * Committen ...
      * View post_aspect_sentiment muss dann nochmal aufgefächert werden: post_aspect_sentiment_always, post_aspect_sentiment_linear_distance, post_aspect_sentiment_grammar_distance
            * STEHENGEBLIEBEN! Links stehendes View post_phrase_aspect_sentiment schön machen und in migration script. Einmal dist ignorieren und für linear_distance tresholden (z. B. 3).
      * Sollen wir in post_phrase_polarity auf polarity phrase verweisen (mit in Primary key?)
      * should post_aspect contain n?
      * Lohnen sich separate Tabellen post_sentiment_document und post_sentiment_sentence wirklich? Vllt einfach sentence_index bei Bedarf auf NULL
      * 
      */

    CREATE TABLE absa.post_phrase_polarity (
        source TEXT, post_id TEXT,
        n INT, word_index INT,
        FOREIGN KEY (source, post_id, n, word_index)
            REFERENCES absa.post_ngram,
        polarity REAL,
        stddev REAL,
        dataset TEXT,
        match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, n, word_index,
            dataset, match_algorithm
        )
    );


    -- TODO: Zusammmenlegen mit post_sentiment_sentence?
    CREATE TABLE absa.post_sentiment_document (
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



    CREATE TABLE absa.post_sentiment_sentence (
        source TEXT,
        post_id TEXT,
        sentence_index INT NOT NULL,
        CHECK (sentence_index >= 0),
        sentiment REAL,
        CHECK (sentiment BETWEEN -1 AND 1),
        stddev REAL,
        subjectivity REAL,
        CHECK (subjectivity BETWEEN -1 AND 1),
        count INT,
        dataset TEXT,
        match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, sentence_index,
            dataset, match_algorithm
        )
    );

    CREATE VIEW absa.post_sentiment AS (
        (
            SELECT
                source, post_id,
                NULL AS sentence_index,
                sentiment,
                stddev,
                subjectivity,
                count,
                dataset,
                match_algorithm,
                'same_document' AS sentiment_model
            FROM
                absa.post_sentiment_document
        ) UNION (
            SELECT
                source, post_id,
                sentence_index,
                sentiment,
                stddev,
                subjectivity,
                count,
                dataset,
                match_algorithm,
                'same_sentence' AS sentiment_model
            FROM
                absa.post_sentiment_sentence
        )
    );


    CREATE TABLE absa.post_phrase_aspect_polarity (
        source TEXT, post_id TEXT,
        aspect_id INT REFERENCES absa.target_aspect,
        aspect_phrase_n INT, aspect_word_index INT,
        FOREIGN KEY (source, post_id, aspect_phrase_n, aspect_word_index)
           REFERENCES absa.post_ngram (source, post_id, n, word_index),
        aspect_sentence_index INT,
        polarity_phrase_n INT, polarity_word_index INT,
        FOREIGN KEY (source, post_id, polarity_phrase_n, polarity_word_index)
           REFERENCES absa.post_ngram (source, post_id, n, word_index),
        polarity_sentence_index INT,
        polarity REAL,
        count INT,
        dataset TEXT,
        aspect_match_algorithm TEXT,
        sentiment_match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, aspect_id,
            aspect_phrase_n, aspect_word_index,
            polarity_phrase_n, polarity_word_index,
            dataset, aspect_match_algorithm, sentiment_match_algorithm
        )
    );


    CREATE TABLE absa.post_phrase_aspect_polarity_linear_distance (
        source TEXT, post_id TEXT,
        aspect_id INT REFERENCES absa.target_aspect,
        aspect_word_index INT,
        FOREIGN KEY (source, post_id, aspect_word_index)
            REFERENCES absa.post_word,  -- TODO: Add missing n
        polarity_phrase_n INT, polarity_word_index INT,
        FOREIGN KEY (source, post_id, polarity_phrase_n, polarity_word_index)
            REFERENCES absa.post_ngram (source, post_id, n, word_index),
        polarity REAL,
        linear_distance INT,
        count INT,
        dataset TEXT,
        aspect_match_algorithm TEXT,
        sentiment_match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, aspect_id,
            aspect_word_index,
            polarity_word_index, polarity_phrase_n,
            dataset, aspect_match_algorithm, sentiment_match_algorithm
        )
    );

    /* CREATE TABLE absa.post_aspect_sentiment (
        source TEXT, post_id TEXT,
        --FOREIGN KEY (source, post_id) REFERENCES post,
        aspect_id INT REFERENCES absa.target_aspect,
        sentiment REAL,
        count INT NOT NULL,
        dataset TEXT NOT NULL,
        aspect_match_algorithm TEXT NOT NULL,
        sentiment_match_algorithm TEXT NOT NULL,
        sentiment_model TEXT NOT NULL,
        PRIMARY KEY (
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm,
            sentiment_match_algorithm, sentiment_model
        )
    ); */
    CREATE VIEW absa.post_aspect_sentiment_max_sentence AS (
        SELECT
            source, post_id,
            aspect_id,
             CASE
                WHEN sum(polarity) > 0
                THEN sum(polarity ^ 2) / sum(polarity)
                ELSE NULL
            END AS sentiment,
            count(DISTINCT polarity_word_index) AS count,
            dataset,
            aspect_match_algorithm,
            sentiment_match_algorithm
        FROM absa.post_phrase_aspect_polarity
        WHERE polarity_sentence_index = aspect_sentence_index
        GROUP BY
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm,
            sentiment_match_algorithm
    );

    CREATE VIEW absa.post_aspect_sentiment_max_document AS (
        SELECT
            source, post_id,
            aspect_id,
            CASE
                WHEN sum(polarity) > 0
                THEN sum(polarity ^ 2) / sum(polarity)
                ELSE NULL
            END AS sentiment,
            count(DISTINCT polarity_word_index) AS count,
            dataset,
            aspect_match_algorithm,
            sentiment_match_algorithm
        FROM absa.post_phrase_aspect_polarity
        GROUP BY
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm,
            sentiment_match_algorithm
    );

    CREATE VIEW absa.post_aspect_sentiment_max AS (
        (
            SELECT *, 'same_document' AS sentiment_model
            FROM absa.post_aspect_sentiment_max_document
        ) UNION (
            SELECT *, 'same_sentence' AS sentiment_model
            FROM absa.post_aspect_sentiment_max_sentence
        )
    );


    CREATE TABLE absa.post_aspect_sentiment_linear_distance_limit (
        source TEXT, post_id TEXT,
        aspect_id INT REFERENCES absa.target_aspect,
        linear_distance INT,
        sentiment REAL,
        aspect_count INT, polarity_count INT,
        dataset TEXT,
        aspect_match_algorithm TEXT, sentiment_match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm, sentiment_match_algorithm
        )
    );

    CREATE TABLE absa.post_aspect_sentiment_linear_distance_weight (
        source TEXT, post_id TEXT,
        aspect_id INT REFERENCES absa.target_aspect,
        linear_distance INT,
        sentiment REAL,
        aspect_count INT, polarity_count INT,
        dataset TEXT,
        aspect_match_algorithm TEXT, sentiment_match_algorithm TEXT,
        PRIMARY KEY (
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm, sentiment_match_algorithm
        )
    );

    CREATE VIEW absa.post_aspect_sentiment_linear_distance AS (
        (
            SELECT
                *,
                'limit' AS distance_method
            FROM
                absa.post_aspect_sentiment_linear_distance_limit
        ) UNION (
            SELECT
                *,
                'weight' AS distance_method
            FROM
                absa.post_aspect_sentiment_linear_distance_weight
        )
    );


    CREATE VIEW absa.post_aspect_sentiment AS (
        (
            SELECT
                source, post_id, aspect_id,
                sentiment,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm,
                sentiment_model
            FROM
                absa.post_aspect_sentiment_max
        ) UNION (
            SELECT
                source, post_id, aspect_id,
                sentiment,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm,
                'linear_distance' AS sentiment_model
            FROM
                absa.post_aspect_sentiment_linear_distance
        )
    );

COMMIT;

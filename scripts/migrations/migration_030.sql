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


    CREATE VIEW absa.post_phrase_aspect_polarity AS (
        SELECT
            polarity_phrase.source, polarity_phrase.post_id,
            aspect_id,
            aspect_phrase.n AS aspect_phrase_n,
            aspect_phrase.word_index AS aspect_word_index,
            aspect_phrase.sentence_index AS aspect_sentence_index,
            polarity_phrase.n AS polarity_phrase_n,
            polarity_phrase.word_index AS polarity_word_index,
            polarity_phrase.sentence_index AS polarity_sentence_index,
            avg(polarity) AS polarity,
            count(DISTINCT post_aspect.word_index) AS count,
            dataset,
            post_aspect.match_algorithm AS aspect_match_algorithm,
            post_phrase_polarity.match_algorithm AS sentiment_match_algorithm
        FROM absa.post_phrase_polarity
            JOIN absa.post_ngram AS polarity_phrase
                USING (source, post_id, n, word_index)
            JOIN absa.post_aspect USING (source, post_id)
            JOIN absa.post_ngram AS aspect_phrase ON
                (
                    aspect_phrase.source,
                    aspect_phrase.post_id,
                    aspect_phrase.word_index
                ) = (
                    post_aspect.source,
                    post_aspect.post_id,
                    post_aspect.word_index
                )
        GROUP BY
            polarity_phrase.source, polarity_phrase.post_id,
            aspect_id,
            aspect_phrase.n, aspect_phrase.word_index,
            polarity_phrase.n, polarity_phrase.word_index,
            dataset, aspect_match_algorithm,
            sentiment_match_algorithm,
            -- Pseudo joins:
            aspect_phrase.sentence_index, polarity_phrase.sentence_index
    );


    CREATE VIEW absa.post_phrase_aspect_polarity_linear_distance AS (
        WITH linear_distance AS (
            SELECT
                post_phrase_aspect_polarity.source,
                post_phrase_aspect_polarity.post_id,
                aspect_id,
                aspect_word_index,
                polarity_word_index, polarity_phrase_n,
                avg(polarity) AS polarity,
                least(
                    min(abs(polarity_word_index - (
                        aspect_word_index + aspect_phrase_n - 1)
                    )),
                    min(abs(aspect_word_index - (
                        polarity_word_index + polarity_phrase_n - 1)
                    ))
                ) AS linear_distance,
                sum(count) AS sum,
                dataset,
                aspect_match_algorithm,
                sentiment_match_algorithm
            FROM
                absa.post_phrase_aspect_polarity
            GROUP BY
                post_phrase_aspect_polarity.source,
                post_phrase_aspect_polarity.post_id,
                aspect_id,
                aspect_word_index,
                polarity_word_index,
                polarity_phrase_n,
                dataset, aspect_match_algorithm,
                sentiment_match_algorithm
        )
        SELECT *
        FROM linear_distance
        WHERE linear_distance <= 4
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
            avg(polarity) AS sentiment,
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
            avg(polarity) AS sentiment,
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


    CREATE VIEW absa.post_aspect_sentiment_linear_distance AS (
        SELECT
            source, post_id,
            aspect_id,
            linear_distance,
            avg(polarity) AS sentiment,
            count(DISTINCT aspect_word_index) AS aspect_count,
            count(DISTINCT polarity_word_index) AS polarity_count,
            dataset,
            aspect_match_algorithm,
            sentiment_match_algorithm
        FROM absa.post_phrase_aspect_polarity_linear_distance
        GROUP BY
            source, post_id, aspect_id,
            dataset, aspect_match_algorithm,
            sentiment_match_algorithm,
            -- pseudo group
            linear_distance
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

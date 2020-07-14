-- ABSA: Add sentence_index to post_word and post_ngram relations (!257)
-- Also raise word_index to start at 1.

BEGIN;

    TRUNCATE absa.post_word CASCADE;
    ALTER TABLE absa.post_word
        ADD COLUMN sentence_index INT NOT NULL
            CHECK (sentence_index > 0),
        ADD CONSTRAINT post_word_word_index_check
            CHECK (word_index > 0);

    TRUNCATE absa.post_ngram;
    ALTER TABLE absa.post_ngram
        ADD COLUMN sentence_index INT NOT NULL
            CHECK (sentence_index > 0),
        ADD CONSTRAINT post_ngram_word_index_check
            CHECK (word_index > 0);

COMMIT;

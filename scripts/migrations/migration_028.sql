-- ABSA: Add sentence_index to post_word and post_ngram relations (!257)

BEGIN;

    TRUNCATE absa.post_word CASCADE;
    ALTER TABLE absa.post_word
        ADD COLUMN sentence_index INT NOT NULL;

    TRUNCATE absa.post_ngram;
    ALTER TABLE absa.post_ngram
        ADD COLUMN sentence_index INT NOT NULL;

COMMIT;

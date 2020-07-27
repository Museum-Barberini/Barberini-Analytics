-- Reorganize topic_modeling tables (!231)

BEGIN;

    CREATE SCHEMA topic_modeling;

    ALTER TABLE topic_modeling_texts
        SET SCHEMA topic_modeling;
    ALTER TABLE topic_modeling.topic_modeling_texts
        RENAME TO topic_text;
    ALTER TABLE topic_modeling.topic_text
        RENAME CONSTRAINT topic_modeling_texts_pkey TO topic_text_pkey;

    ALTER TABLE topic_modeling_topics
        SET SCHEMA topic_modeling;
    ALTER TABLE topic_modeling.topic_modeling_topics
        RENAME TO topic;
    ALTER TABLE topic_modeling.topic
        RENAME CONSTRAINT topic_modeling_topics_pkey TO topic_pkey;

COMMIT;

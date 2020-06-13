-- Reorganize topic_modeling tables (!231)

BEGIN;

    CREATE SCHEMA topic_modeling;

    ALTER TABLE topic_modeling_texts
        SET SCHEMA topic_modeling;
    ALTER TABLE topic_modeling.topic_modeling_texts
        RENAME TO topic_text;
    ALTER TABLE topic_modeling_topics
        SET SCHEMA topic_modeling;
    ALTER TABLE topic_modeling.topic_modeling_topics
        RENAME TO topic;

COMMIT;

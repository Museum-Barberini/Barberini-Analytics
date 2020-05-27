-- Re-introduce 'fb_post_id' into fb_post_performance
-- for simple unchanged-values-check

BEGIN;

ALTER TABLE fb_post_performance
    ADD COLUMN fb_post_id TEXT
        GENERATED  ALWAYS AS (
            page_id || '_' || post_id
        ) STORED;

COMMIT;

/** Truncate facebook posts (#213)
    We want to do this because in the past, we have collected a number of old
    posts that are not necessarily authored by the museum itself. To avoid
    wrong is_from_museum attributions, we drop all posts so that posts that
    are indeed by the museum will be fetched again automatically (see #184).
  */

BEGIN;

    /** In order to keep performance data, we will do the some SQL acrobatics
        below.
      */

    -- 1. Decouple performance table from post table
    ALTER TABLE fb_post_performance
        DROP CONSTRAINT fb_post_performance_fb_post_id_fkey;
    -- 2. Truncate post table
    TRUNCATE TABLE fb_post;
    -- 3. Reconnect the performance table
    ALTER TABLE fb_post_performance
        ADD FOREIGN KEY (page_id, post_id) REFERENCES fb_post;

COMMIT;

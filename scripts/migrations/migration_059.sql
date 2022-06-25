-- Fix Apple Appstore reviews permalink (!448)

BEGIN;

    -- 1. Disconnect referencing views
    CREATE OR REPLACE VIEW app_review AS (
        SELECT *
        FROM app_review
    );

    -- 2. Recreate generated column
    ALTER TABLE appstore_review
        DROP COLUMN permalink,
        ADD COLUMN permalink TEXT GENERATED ALWAYS AS (
                'https://apps.apple.com/de/app/museum-barberini/id'
                    || app_id
                    || '?see-all=reviews'
                -- Apple seems not to support review-specific URLs
            ) STORED;

    -- 3. Reconnect referencing views (NO CHANGE)
    CREATE OR REPLACE VIEW app_review AS
    (
        SELECT
            'Apple Appstore' AS source,
            appstore_review_id AS review_id,
            NULLIF(CONCAT_WS(E'\n', title, text), '') AS text,
            post_date,
            rating,
            app_version,
            NULL AS likes,
            title,
            permalink
        FROM appstore_review
        WHERE app_id = '1150432552'  -- Museum Barberini
    ) UNION (
        SELECT
            'Google Play' AS source,
            playstore_review_id AS review_id,
            text,
            post_date,
            rating,
            app_version,
            thumbs_up AS likes,
            NULL as title,
            permalink
        FROM gplay_review
        WHERE app_id = 'com.barberini.museum.barberinidigital'
    );

COMMIT;

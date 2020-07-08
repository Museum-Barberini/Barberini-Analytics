-- Fix app_reviews title of gplay reviews (!278)

BEGIN;

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
            title,
            permalink
        FROM gplay_review
        WHERE app_id = 'com.barberini.museum.barberinidigital'
    );

COMMIT;

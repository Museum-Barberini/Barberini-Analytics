-- Fix degenerated Google Maps review IDs (!449)

BEGIN;

    /* No idea where these degenerated IDs came from, but a manual analysis
       confirmed that for each of these IDs, a valid ID with the same data
       exists, so we can safely drop the degenerated and redundat items. The
       current revision of the connector does not generate the falsy IDs any
       longer. */
    DELETE FROM google_maps_reviews
        WHERE google_maps_review_id LIKE '%==';

COMMIT;

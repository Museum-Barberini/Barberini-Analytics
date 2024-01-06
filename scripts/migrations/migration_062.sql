-- Instagram: Rename "engagement" column to "total_interactions"

BEGIN;

    ALTER TABLE ig_post_performance
        RENAME COLUMN engagement TO total_interactions;
    ALTER TABLE ig_post_performance
        RENAME COLUMN delta_engagement TO delta_total_interactions;

END;

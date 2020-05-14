/** Add missing service-specific museum IDs
    In the past, we only fetched posts from these sources for the museum,
    but we did not note its ID. In migration_011, the missing columns were
    added, but there it was forgotten to update the existing rows. This is
    done here.
  */

BEGIN;

    UPDATE appstore_review
        SET app_id = '1150432552';
    UPDATE gplay_review
        SET app_id = 'com.barberini.museum.barberinidigital';

    UPDATE google_maps_review
        SET place_id = 'ChIJyV9mg0lfqEcRnbhJji6c17E';

COMMIT;

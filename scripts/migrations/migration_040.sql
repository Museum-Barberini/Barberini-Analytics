-- Nuke empty gplay reviews title (!278)

BEGIN;

    ALTER TABLE gplay_review
        DROP COLUMN title;

COMMIT;

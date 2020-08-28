-- Clean up invalid column values in tweet table (!355)

BEGIN;

    UPDATE tweet
        SET response_to = NULL
        WHERE response_to LIKE '%e+%';

COMMIT;

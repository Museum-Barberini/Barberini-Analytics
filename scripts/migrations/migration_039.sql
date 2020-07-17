-- Visitor prediction - add essential relations (!301)

BEGIN;

    CREATE TABLE visitor_prediction (
        is_sample BOOLEAN,
        timespan INT,
        date DATE,
        entries INT,
        PRIMARY KEY (
            is_sample, timespan, date
        )
    );

COMMIT;

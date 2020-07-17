-- Visitor prediction - add essential relations (!301)

BEGIN;

    --- multiple independent sets of predictions are stored in the same relation
    --- with different timespans and sometimes predicting past values (samples)
    --- to provide a reference
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

-- Integrate gomus quotas and capacities (!377)

BEGIN;

    CREATE TABLE gomus_quota (
        gomus_quota_id INT PRIMARY KEY,
        name TEXT,
        creation_date TIMESTAMP,
        update_date TIMESTAMP
    );

COMMIT;

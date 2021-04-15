-- Support removal of gomus quotas (!403)

BEGIN;

    ALTER TABLE gomus_capacity
        DROP CONSTRAINT gomus_capacity_quota_id_fkey,
        ADD CONSTRAINT gomus_capacity_quota_id_fkey
        FOREIGN KEY (quota_id)
        REFERENCES gomus_quota
        ON DELETE CASCADE;

END;

-- Update gomus capacity checks for overbooked quotas

BEGIN;

    ALTER TABLE gomus_capacity
        DROP CONSTRAINT gomus_capacity_check;
    ALTER TABLE gomus_capacity
        ADD CONSTRAINT gomus_capacity_check
        CHECK (
            -- old computation (negative available)
            available < 0 AND max - sold - reserved = available
            OR
            -- new computation (clamped available)
            available >= 0 AND GREATEST(max - sold - reserved, 0) = available
        );

END;

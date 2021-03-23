-- Fix ForeignKeyViolation in EnhanceBookingsWithScraper (#372)

BEGIN;

    ALTER TABLE gomus_booking
        DROP CONSTRAINT gomus_booking_customer_id_fkey,
        ADD CONSTRAINT gomus_booking_customer_id_fkey
            FOREIGN KEY (customer_id)
            REFERENCES gomus_customer
            ON UPDATE CASCADE;

COMMIT;

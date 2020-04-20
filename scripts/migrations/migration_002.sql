BEGIN;

    DROP TABLE gomus_event;

    CREATE TABLE gomus_event (
        event_id INTEGER,
        booking_id INTEGER,
        customer_id INTEGER,
        reservation_count INTEGER,
        order_date DATE,
        status TEXT,
        category TEXT
    );
    ALTER TABLE gomus_event
        ADD CONSTRAINT gomus_event_primkey PRIMARY KEY (event_id);
    ALTER TABLE gomus_event
        ADD CONSTRAINT booking_id_fkey FOREIGN KEY (booking_id) REFERENCES gomus_booking (booking_id);
    ALTER TABLE gomus_event
        ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id);

    DROP TABLE tweet_author;

    CREATE TABLE tweet_author (
        user_id TEXT, 
        user_name TEXT
    );
    ALTER TABLE tweet_author
        ADD CONSTRAINT tweet_author_primkey PRIMARY KEY (user_id);

COMMIT;

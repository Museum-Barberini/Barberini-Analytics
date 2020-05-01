/* Initial migration script
 * Before the migration system was introduced, all relations were created via
 * the task architecture. This is script restores the schematic database
 * state from the time where the migration system was introduced.
 * Actually, this wasn't a binary update in architecture but rather a
 * continuous changeover. For this reason, further declarations such as
 * foreign keys were later added to this script.
 */

BEGIN;

    CREATE TABLE appstore_review (
        appstore_review_id TEXT,
        text TEXT,
        rating INTEGER,
        app_version TEXT,
        vote_count INTEGER,
        vote_sum INTEGER,
        title TEXT,
        date TIMESTAMP,
        country_code TEXT
    );
    ALTER TABLE appstore_review
        ADD CONSTRAINT appstore_review_primkey PRIMARY KEY (appstore_review_id);


    -- Added post hoc to complement !115 (exhibitions)
    CREATE TABLE exhibition (
        title TEXT,
        start_date DATE,
        end_date DATE
    );
    ALTER TABLE exhibition
        ADD CONSTRAINT exhibition_primkey PRIMARY KEY (title, start_date);


    CREATE TABLE fb_post (
        post_date TIMESTAMP,
        text TEXT,
        fb_post_id TEXT
    );
    ALTER TABLE fb_post
        ADD CONSTRAINT fb_post_primkey PRIMARY KEY (fb_post_id);

    CREATE TABLE fb_post_performance (
        fb_post_id TEXT,
        time_stamp TIMESTAMP,
        react_like INTEGER,
        react_love INTEGER,
        react_wow INTEGER,
        react_haha INTEGER,
        react_sorry INTEGER,
        react_anger INTEGER,
        likes INTEGER,
        shares INTEGER,
        comments INTEGER,
        video_clicks INTEGER,
        link_clicks INTEGER,
        other_clicks INTEGER,
        negative_feedback INTEGER,
        paid_impressions INTEGER
    );
    ALTER TABLE fb_post_performance
        ADD CONSTRAINT fb_post_performance_primkey PRIMARY KEY (fb_post_id, time_stamp);
    -- Added post hoc as part of extraction of foreign keys from the code
    ALTER TABLE fb_post_performance
        ADD CONSTRAINT fb_post_id_fkey FOREIGN KEY (fb_post_id) REFERENCES fb_post (fb_post_id);


    CREATE TABLE gomus_booking (
        booking_id INTEGER,
        customer_id INTEGER,
        category TEXT,
        participants INTEGER,
        guide_id INTEGER,
        duration INTEGER,
        exhibition TEXT,
        title TEXT,
        status TEXT,
        start_datetime TIMESTAMP,
        order_date DATE,
        language TEXT
    );
    ALTER TABLE gomus_booking
        ADD CONSTRAINT gomus_booking_primkey PRIMARY KEY (booking_id);

    CREATE TABLE gomus_customer (
        customer_id INTEGER,
        postal_code TEXT,  -- e.g. non-german
        newsletter BOOLEAN,
        gender TEXT,
        category TEXT,
        language TEXT,
        country TEXT,
        type TEXT,  -- shop, shop guest or normal
        register_date DATE,
        annual_ticket BOOLEAN,
        valid_mail BOOLEAN
    );
    ALTER TABLE gomus_customer
        ADD CONSTRAINT gomus_customer_primkey PRIMARY KEY (customer_id);

    CREATE TABLE gomus_daily_entry (
        id INTEGER,
        ticket TEXT,
        datetime TIMESTAMP,
        count INTEGER
    );
    ALTER TABLE gomus_daily_entry
        ADD CONSTRAINT gomus_daily_entry_primkey PRIMARY KEY (id, datetime);

    CREATE TABLE gomus_event (
        event_id INTEGER,
        customer_id INTEGER,
        booking_id INTEGER,
        reservation_count INTEGER,
        order_date DATE,
        status TEXT,
        category TEXT
    );
    ALTER TABLE gomus_event
        ADD CONSTRAINT gomus_event_primkey PRIMARY KEY (event_id);

    CREATE TABLE gomus_expected_daily_entry (
        id INTEGER,
        ticket TEXT,
        datetime TIMESTAMP,
        count INTEGER
    );
    ALTER TABLE gomus_expected_daily_entry
        ADD CONSTRAINT gomus_expected_daily_entry_primkey PRIMARY KEY (id, datetime);

    CREATE TABLE gomus_order (
        order_id INTEGER,
        order_date DATE,
        customer_id INTEGER,
        valid BOOLEAN,
        paid BOOLEAN,
        origin TEXT
    );
    ALTER TABLE gomus_order
        ADD CONSTRAINT gomus_order_primkey PRIMARY KEY (order_id);

    CREATE TABLE gomus_order_contains (
        article_id INTEGER,
        -- Added post hoc as part of extraction of db declarations from the code
        article_type TEXT,
        order_id INTEGER,
        ticket TEXT,
        date DATE,
        quantity INTEGER,
        price FLOAT
    );
    ALTER TABLE gomus_order_contains
        ADD CONSTRAINT gomus_order_contains_primkey PRIMARY KEY (article_id);

    CREATE TABLE gomus_to_customer_mapping (
        gomus_id INTEGER,
        customer_id INTEGER
    );
    ALTER TABLE gomus_to_customer_mapping
        ADD CONSTRAINT gomus_to_customer_mapping_primkey PRIMARY KEY (gomus_id);

    ALTER TABLE gomus_booking
        ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id);
    ALTER TABLE gomus_event
        ADD CONSTRAINT booking_id_fkey FOREIGN KEY (booking_id) REFERENCES gomus_booking (booking_id);
    ALTER TABLE gomus_order
        ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id);
    ALTER TABLE gomus_order_contains
        ADD CONSTRAINT order_id_fkey FOREIGN KEY (order_id) REFERENCES gomus_order (order_id);
    -- Added post hoc as part of extraction of foreign keys from the code
    ALTER TABLE gomus_to_customer_mapping
        ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id)
        ON UPDATE CASCADE;


    CREATE TABLE google_maps_review (
        google_maps_review_id TEXT,
        date DATE,
        rating INTEGER,
        text TEXT,
        TEXT_english TEXT,
        language TEXT
    );
    ALTER TABLE google_maps_review
        ADD CONSTRAINT google_maps_review_primkey PRIMARY KEY (google_maps_review_id);


    -- Added post hoc to complement gplay reviews (!90)
    CREATE TABLE gplay_review (
        playstore_review_id TEXT,
        text TEXT,
        rating INT,
        app_version TEXT,
        thumbs_up INT,
        title TEXT,
        date TIMESTAMP
    );
    ALTER TABLE gplay_review
        ADD CONSTRAINT gplay_review_primkey PRIMARY KEY (playstore_review_id);


    CREATE TABLE gtrends_value (
        topic TEXT,
        date DATE,
        interest_value INTEGER
    );
    ALTER TABLE gtrends_value
        ADD CONSTRAINT gtrends_value_primkey PRIMARY KEY (topic, date);


    CREATE TABLE tweet (
        user_id TEXT,
        tweet_id TEXT,
        text TEXT,
        response_to TEXT,
        post_date DATE,
        is_from_barberini BOOLEAN
    );
    ALTER TABLE tweet
        ADD CONSTRAINT tweet_primkey PRIMARY KEY (tweet_id);

    CREATE TABLE tweet_performance (
        tweet_id TEXT,
        likes INTEGER,
        retweets INTEGER,
        replies INTEGER,
        timestamp TIMESTAMP
    );
    ALTER TABLE tweet_performance
        ADD CONSTRAINT tweet_performance_primkey PRIMARY KEY (tweet_id, timestamp);
    ALTER TABLE tweet_performance
        ADD CONSTRAINT tweet_id_fkey FOREIGN KEY (tweet_id) REFERENCES tweet (tweet_id);

COMMIT;

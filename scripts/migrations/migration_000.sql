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

CREATE TABLE gomus_booking (
    booking_id INTEGER,
    customer_id integer,
    category text,
    participants integer,
    guide_id integer,
    duration integer,
    exhibition text,
    title text,
    status text,
    start_datetime TIMESTAMP,
    order_date date,
    language text
);
ALTER TABLE gomus_booking
    ADD CONSTRAINT gomus_booking_primkey PRIMARY KEY (booking_id);

CREATE TABLE gomus_customer (
    customer_id integer,
    postal_code text,
    newsletter boolean,
    gender text,
    category text,
    language text,
    country text,
    type text,
    register_date date,
    annual_ticket boolean,
    valid_mail boolean
);
ALTER TABLE gomus_customer
    ADD CONSTRAINT gomus_customer_primkey PRIMARY KEY (customer_id);

CREATE TABLE gomus_daily_entry (
    id integer,
    ticket text,
    datetime timestamp without time zone NOT NULL,
    count integer
);
ALTER TABLE gomus_daily_entry
    ADD CONSTRAINT gomus_daily_entry_primkey PRIMARY KEY (id, datetime);

CREATE TABLE gomus_event (
    event_id integer NOT NULL,
    customer_id integer,
    booking_id integer,
    reservation_count integer,
    order_date date,
    status text,
    category text
);
ALTER TABLE gomus_event
    ADD CONSTRAINT gomus_event_primkey PRIMARY KEY (event_id);

CREATE TABLE google_maps_review (
    google_maps_review_id TEXT,
    date DATE,
    rating INTEGER,
    text TEXT,
    text_english TEXT,
    language TEXT
);
ALTER TABLE google_maps_review
    ADD CONSTRAINT google_maps_review_primkey PRIMARY KEY (google_maps_review_id);

CREATE TABLE gtrends_value (
    topic TEXT,
    date DATE,
    interest_value INTEGER
);
ALTER TABLE gtrends_value
    ADD CONSTRAINT gtrends_value_primkey PRIMARY KEY (topic, date);

ALTER TABLE gomus_booking
    ADD CONSTRAINT customer_id_fkey FOREIGN KEY (customer_id) REFERENCES gomus_customer (customer_id);

ALTER TABLE gomus_event
    ADD CONSTRAINT booking_id_fkey FOREIGN KEY (booking_id) REFERENCES gomus_booking (booking_id);

COMMIT;

BEGIN;

-- Introduce necessary changes to store IG profile
-- metrics and their changes daily


-- Drop old tables
DROP TABLE ig_audience_origin;
DROP TABLE ig_audience_gender_age;


-- Create demographics tables
CREATE TABLE ig_audience_city (
    city TEXT,
    timestamp TIMESTAMP,
    amount INT
);
ALTER TABLE ig_audience_city
    ADD CONSTRAINT ig_audience_city_primkey PRIMARY KEY (city, timestamp);

CREATE TABLE ig_audience_country (
    country TEXT,
    timestamp TIMESTAMP,
    amount INT
);
ALTER TABLE ig_audience_country
    ADD CONSTRAINT ig_audience_country_primkey PRIMARY KEY (country, timestamp);

CREATE TABLE ig_audience_gender_age (
    gender TEXT,
    age TEXT,
    timestamp TIMESTAMP,
    amount INT
);
ALTER TABLE ig_audience_gender_age
    ADD CONSTRAINT ig_audience_gender_age_primkey PRIMARY KEY (gender, age, timestamp);


-- Create profile metrics tables
CREATE TABLE ig_profile_metrics_development (
    timestamp TIMESTAMP,
    impressions INT,
    reach INT,
    profile_views INT,
    follower_count INT,
    website_clicks INT
);
ALTER TABLE ig_profile_metrics_development
    ADD CONSTRAINT ig_profile_metrics_development_primkey PRIMARY KEY (timestamp);

CREATE TABLE ig_total_profile_metrics (
    timestamp TIMESTAMP,
    follower_count INT,
    media_count INT
);
ALTER TABLE ig_total_profile_metrics
    ADD CONSTRAINT ig_total_profile_metrics_primkey PRIMARY KEY (timestamp);

COMMIT;

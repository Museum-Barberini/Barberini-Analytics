ALTER TABLE fb_post RENAME id TO fb_post_id;
ALTER TABLE fb_post_performance RENAME post_id TO fb_post_id;
ALTER TABLE appstore_review RENAME id TO appstore_review_id;
ALTER TABLE appstore_review RENAME content TO text;
ALTER TABLE gomus_customer RENAME id TO gomus_id;
ALTER TABLE gomus_customer RENAME hash_id TO customer_id;
ALTER TABLE gomus_booking RENAME id TO booking_id;
ALTER TABLE gomus_booking RENAME booker_id TO customer_id;
ALTER TABLE gomus_order RENAME id TO order_id;
ALTER TABLE google_maps_review RENAME content TO text_german;
ALTER TABLE google_maps_review RENAME content_original TO text;
ALTER TABLE google_maps_review RENAME id TO google_maps_review_id;
-- ALTER TABLE gomus_event RENAME id TO event_id;
ALTER TABLE tweet_performance ALTER COLUMN timestamp TYPE timestamp;

DROP TABLE gtrends_topics;
DROP TABLE gtrends_interests;
DROP TABLE gtrends_topic;
DROP TABLE gtrends_interest;
-- TODO: Update gtrends schema. We merged two tables! How to handle this here?

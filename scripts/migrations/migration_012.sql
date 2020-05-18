BEGIN;
CREATE TABLE fb_post_comment (
    comment_id TEXT PRIMARY KEY,
    page_id TEXT,
    post_id TEXT,
    post_date TIMESTAMP,
    message TEXT,
    from_barberini BOOLEAN,
    parent TEXT
);

ALTER TABLE fb_post_comment
    ADD CONSTRAINT fb_post_comment_page_id_post_id_fkey FOREIGN KEY (page_id, post_id) REFERENCES fb_post (page_id, post_id);

COMMIT;

BEGIN;
CREATE TABLE fb_post_comment (
    post_id TEXT,
    comment_id TEXT,
    PRIMARY KEY (post_id, comment_id),
    page_id TEXT,
    post_date TIMESTAMP,
    text TEXT,
    is_from_museum BOOLEAN,
    responds_to TEXT,
    permalink TEXT
        GENERATED ALWAYS AS (
            'https://www.facebook.com/' || page_id || '/posts/'
                || post_id || '?comment_id=' || comment_id
        ) STORED,

    FOREIGN KEY (page_id, post_id) REFERENCES fb_post,
    FOREIGN KEY (post_id, responds_to) REFERENCES fb_post_comment
);

COMMIT;

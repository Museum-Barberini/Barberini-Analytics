BEGIN;

    CREATE TABLE absa.target_aspect(
        aspect_id SERIAL PRIMARY KEY,
        aspect text[]
    );

	CREATE TABLE absa.target_aspect_word(
		aspect_id int REFERENCES absa.target_aspect,
		word text,
		PRIMARY KEY (aspect_id, word)
	);

COMMIT;

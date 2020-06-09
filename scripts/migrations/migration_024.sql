BEGIN;

    CREATE TABLE absa.polarity(
        word TEXT PRIMARY KEY,
        pos_tag TEXT,
        polarity REAL,
        inflections TEXT[]
    );

COMMIT;

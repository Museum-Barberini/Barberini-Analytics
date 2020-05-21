-- Add simple word and n-gram tables for ABSA (!135)

BEGIN;

    CREATE SCHEMA absa;

    CREATE PROCEDURE ensure_foreign_key_table(
            "table" regclass,
            key text
        )
        LANGUAGE plpgsql
        AS $$
        BEGIN
            EXECUTE format('
                CREATE
                    OR REPLACE
                FUNCTION foreign_key_trigger()
                    RETURNS "trigger" AS
                    $BODY$ BEGIN
                        -- If refkey is invalid, this will raise an error
                        PERFORM CAST(NEW.%2$s AS regclass);
                        RETURN NEW;
                    END; $BODY$
                    LANGUAGE ''plpgsql'';
                CREATE TRIGGER tr_before_insert_or_update
                    BEFORE INSERT OR UPDATE OF %2$s
                    ON %1$s
                    FOR EACH ROW
                    EXECUTE PROCEDURE foreign_key_trigger();
                ',
                "table", key);
        END;
        $$;

    CREATE TABLE absa.stopword (
        word TEXT PRIMARY KEY
    );

    CREATE TABLE absa.post_word (
        source TEXT,
        post_id TEXT,
        word_index INT,
        word TEXT,
        PRIMARY KEY (source, post_id, word_index)
    );
    CALL ensure_foreign_key_table('absa.post_word', 'source');

    CREATE TABLE absa.post_ngram (
        source TEXT,
        post_id TEXT,
        n INT,
        word_index INT,
        ngram TEXT,
        PRIMARY KEY (source, post_id, n, word_index)
    );
    CALL ensure_foreign_key_table('absa.post_ngram', 'source');

COMMIT;
